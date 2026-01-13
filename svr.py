# server.py - Servidor de Señalización VoIP Avanzado (AsyncIO)
import asyncio
import logging
import time
import hashlib
from typing import Dict, Tuple, Optional, Any
from dataclasses import dataclass
from collections import defaultdict

# --- CONFIGURACIÓN AVANZADA ---
HOST: str = "0.0.0.0"
PORT: int = 20159
MAX_PACKET_SIZE: int = 65535
RATE_LIMIT_WINDOW: float = 1.0  # Segundos
RATE_LIMIT_MAX_REQUESTS: int = 50  # Requests por IP por ventana
CLIENT_TIMEOUT: float = 60.0    # Segundos para timeout de cliente

# --- LOGGING SISTEMA ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("VoIP_Sys")

@dataclass
class ClientInfo:
    ip: str
    port: int
    last_seen: float
    name: str

class RateLimiter:
    def __init__(self):
        self.requests: Dict[str, list] = defaultdict(list)

    def is_allowed(self, ip: str) -> bool:
        now = time.time()
        # Limpiar requests viejos
        self.requests[ip] = [t for t in self.requests[ip] if now - t < RATE_LIMIT_WINDOW]
        
        if len(self.requests[ip]) < RATE_LIMIT_MAX_REQUESTS:
            self.requests[ip].append(now)
            return True
        return False

class SignalingServer:
    def __init__(self):
        self.clients: Dict[str, ClientInfo] = {}  # number -> ClientInfo
        self.claimed_ports: Dict[str, int] = {}   # number -> claimed_port
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.rate_limiter = RateLimiter()
        self.dedup_cache: Dict[Tuple[str, str], float] = {} # ((ip, port), hash) -> timestamp

    def connection_made(self, transport: asyncio.DatagramTransport):
        self.transport = transport
        logger.info(f"🚀 Sistema VoIP Iniciado en {HOST}:{PORT}")
        logger.info(f"⚙️  Modelo: AsyncIO High-Performance | RateLimit: {RATE_LIMIT_MAX_REQUESTS}req/s")

    def connection_lost(self, exc: Optional[Exception]):
        logger.error(f"❌ Conexión terminada: {exc}")

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        # 1. Rate Limiting
        if not self.rate_limiter.is_allowed(addr[0]):
            if len(self.rate_limiter.requests[addr[0]]) == RATE_LIMIT_MAX_REQUESTS:
                logger.warning(f"🛡️ Rate Limit Excedido: {addr[0]}")
            return

        # 2. Decodificación
        try:
            msg = data.decode(errors="ignore").strip()
        except:
            return

        # 3. Deduplicación (Anti-Replay / Redundancy check)
        # Check simple para evitar procesar el mismo paquete (por redundancia de red)
        msg_hash = hashlib.md5(msg.encode()).hexdigest()
        dedup_key = (f"{addr[0]}:{addr[1]}", msg_hash)
        now = time.time()
        last_time = self.dedup_cache.get(dedup_key, 0)
        
        # Si recibimos el mismo hash en menos de 300ms, lo ignoramos
        if now - last_time < 0.3:
            return
        
        self.dedup_cache[dedup_key] = now
        # Limpieza perezosa del cache de dedup (super simple)
        if len(self.dedup_cache) > 5000:
            self.dedup_cache.clear()

        # 4. Procesamiento
        asyncio.create_task(self.handle_command(msg, addr))

    def _send(self, data: bytes, addr: Tuple[str, int], copies: int = 1):
        if not self.transport: return
        try:
            for _ in range(copies):
                self.transport.sendto(data, addr)
        except Exception as e:
            logger.error(f"Error enviando a {addr}: {e}")

    async def handle_command(self, msg: str, addr: Tuple[str, int]):
        parts = msg.split(":")
        cmd = parts[0]
        
        # Actualizar "last_seen" si ya conocemos el cliente por IP
        # (Opcional, pero bueno para mantener vivas las sesiones NAT)

        if cmd == "REGISTER":
            # REGISTER:number:claimed_port:name
            number = parts[1] if len(parts) > 1 else ""
            c_port = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            name = parts[3] if len(parts) > 3 else ""
            
            number = number[:32]
            name = name[:32]
            
            self.clients[number] = ClientInfo(addr[0], addr[1], time.time(), name)
            if c_port > 0:
                self.claimed_ports[number] = c_port
                
            logger.info(f"✅ REGISTRADO: {number} ({name}) -> {addr[0]}:{addr[1]}")
            self._send(b"OK", addr, copies=2)

        elif cmd == "PING":
            number = parts[1] if len(parts) > 1 else ""
            if number in self.clients:
                self.clients[number].last_seen = time.time()
                # Actualizar IP/Port por si cambió NAT
                self.clients[number].ip = addr[0]
                self.clients[number].port = addr[1]
                
            self._send(b"PONG", addr, copies=1)

        elif cmd == "CALL":
            # CALL:callee:caller
            callee = parts[1] if len(parts) > 1 else ""
            caller = parts[2] if len(parts) > 2 else ""
            
            logger.info(f"📞 CALL {caller} -> {callee}")
            
            caller_name = ""
            if caller in self.clients:
                caller_name = self.clients[caller].name

            # Forwarding
            sent = await self._forward(callee, f"CALL_FROM:{caller}:{caller_name}")
            if sent:
                self._send(b"OK", addr)
                # Ringing feedback
                self._send(f"RINGING_FROM:{callee}".encode(), addr, copies=2)
            else:
                self._send(f"OFFLINE:{callee}".encode(), addr, copies=2)

        elif cmd in ("ACCEPT", "REJECT", "BUSY", "BYE"):
            # GENERIC FORWARDING COMMANDS
            # CMD:target:source
            target = parts[1] if len(parts) > 1 else ""
            source = parts[2] if len(parts) > 2 else ""
            
            logger.info(f"➡️  {cmd} {source} -> {target}")
            
            payload = f"{cmd}_FROM:{source}" # Ej: ACCEPT_FROM:123
            sent = await self._forward(target, payload)
            if sent:
                self._send(b"OK", addr)
            else:
                self._send(f"OFFLINE:{source}".encode(), addr)

        elif cmd in ("OFFER_B64", "ANSWER_B64", "ICE_B64"):
            # WebRTC Signaling / Large Payload Forwarding
            # CMD:target:source:<data>
            if len(parts) >= 4:
                target = parts[1]
                source = parts[2]
                payload_data = ":".join(parts[3:])
                
                # logger.debug(f"📡 {cmd} size={len(payload_data)} bytes") # Verbose
                
                # Reconstruir mensaje
                out_cmd = cmd.replace("_B64", "_FROM_B64") # OFFER_FROM_B64
                if await self._forward(target, f"{out_cmd}:{source}:{payload_data}"):
                    self._send(b"OK", addr)
            else:
                self._send(b"ERR:MALFORMED", addr)

        elif cmd == "AUDIO_B64":
             # Audio rápido, sin logs excesivos
            if len(parts) >= 4:
                target = parts[1]
                source = parts[2]
                payload_data = ":".join(parts[3:])
                await self._forward(target, f"AUDIO_FROM_B64:{source}:{payload_data}")

        elif cmd == "SMS_PRIVATE":
            # SMS_PRIVATE:target:source:msg
            # Forward -> SMS_PRIVATE_FROM:source:name:msg
            if len(parts) >= 4:
                target = parts[1]
                source = parts[2]
                msg_content = ":".join(parts[3:])
                
                source_name = "Unknown"
                if source in self.clients:
                    source_name = self.clients[source].name
                
                logger.info(f"📩 SMS PRIV {source} -> {target}")
                sent = await self._forward(target, f"SMS_PRIVATE_FROM:{source}:{source_name}:{msg_content}")
                if sent:
                    self._send(b"OK", addr)
                else:
                    self._send(f"OFFLINE:{target}".encode(), addr)

        elif cmd == "SMS_GLOBAL":
            # SMS_GLOBAL:source:msg
            # Broadcast -> SMS_GLOBAL_FROM:source:name:msg
            if len(parts) >= 3:
                source = parts[1]
                msg_content = ":".join(parts[2:])
                
                source_name = "Unknown"
                if source in self.clients:
                    source_name = self.clients[source].name
                
                logger.info(f"📢 SMS GLOBAL from {source}")
                
                payload = f"SMS_GLOBAL_FROM:{source}:{source_name}:{msg_content}".encode()
                
                # Broadcast efficiently
                for num, client in self.clients.items():
                    if num == source: continue # Don't echo back to sender if they handle it locally
                    self._send(payload, (client.ip, client.port))
                    
                self._send(b"OK", addr)

        elif cmd == "LIST":
            # Devolver lista de conectados
            active_users = [f"{n}|{c.name}" for n, c in self.clients.items()]
            resp = "LIST:" + ",".join(active_users)
            self._send(resp.encode(), addr, copies=2)
            
        elif cmd == "UNREGISTER":
            number = parts[1] if len(parts) > 1 else ""
            if number in self.clients:
                del self.clients[number]
                logger.info(f"👋 UNREGISTER {number}")
            self._send(b"OK", addr)

    async def _forward(self, target_number: str, payload_str: str) -> bool:
        if target_number not in self.clients:
            return False
            
        client = self.clients[target_number]
        payload = payload_str.encode()
        
        # Enviar a la dirección registrada
        self._send(payload, (client.ip, client.port), copies=2)
        
        # Si tiene un "Claimed Port" (puerto fijo mapeado), intentar también allí
        # Esto ayuda mucho con NATs estrictos si el cliente sabe su puerto externo
        c_port = self.claimed_ports.get(target_number, 0)
        if c_port > 0 and c_port != client.port:
             self._send(payload, (client.ip, c_port), copies=2)
             
        return True

    async def cleanup_loop(self):
        """Tarea en background para limpiar clientes inactivos"""
        logger.info("🧹 Garbage Inspector activo")
        while True:
            await asyncio.sleep(30)
            now = time.time()
            expired = []
            for num, info in self.clients.items():
                if now - info.last_seen > CLIENT_TIMEOUT:
                    expired.append(num)
            
            for num in expired:
                logger.info(f"💤 Timeout de cliente: {num}")
                del self.clients[num]
                if num in self.claimed_ports:
                    del self.claimed_ports[num]

async def main():
    loop = asyncio.get_running_loop()
    server = SignalingServer()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: server,
        local_addr=(HOST, PORT)
    )

    # Iniciar tareas de mantenimiento
    asyncio.create_task(server.cleanup_loop())

    try:
        await asyncio.Future()  # Run forever
    except asyncio.CancelledError:
        pass
    finally:
        transport.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Servidor detenido manualmente")
