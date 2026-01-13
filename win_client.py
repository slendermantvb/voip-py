import socket
import threading
import time
import base64
import logging
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import sys
import random
import array
import json
import os
from queue import Queue
from typing import Optional
from datetime import datetime

# --- CONFIGURACIÓN DE LOGS ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("VoIPClient")

# --- DEPENDENCIAS ---
PYAUDIO_AVAILABLE = False
try:
    import pyaudio
    PYAUDIO_AVAILABLE = True
except ImportError:
    logger.warning("PyAudio no instalado")

WINSOUND_AVAILABLE = False
try:
    import winsound
    WINSOUND_AVAILABLE = True
except ImportError:
    pass

# --- CONSTANTES ---
AUDIO_RATE = 16000
AUDIO_CHUNK = 320
JITTER_BUFFER_SIZE = 20
DISCONNECT_TIMEOUT = 30.0

# --- ESTILOS VISUALES (AMOLED DARK) ---
class UIColors:
    BG_MAIN = "#000000"         # Fondo Principal
    BG_CARD = "#1C1C1E"         # Fondo Tarjetas/Items
    BG_HOVER = "#2C2C2E"        # Hover State
    TEXT_MAIN = "#FFFFFF"       # Texto Primario
    TEXT_SEC = "#8E8E93"        # Texto Secundario (Gris)
    ACCENT = "#0A84FF"          # Azul (iOS style)
    SUCCESS = "#30D158"         # Verde Llamada
    DANGER = "#FF453A"          # Rojo Colgar
    DIVIDER = "#38383A"         # Líneas divisorias
    
class UIFonts:
    H1 = ("Segoe UI", 32, "bold")
    H2 = ("Segoe UI", 24)
    H3 = ("Segoe UI", 16, "bold")
    BODY = ("Segoe UI", 12)
    SMALL = ("Segoe UI", 10)
    ICON_LG = ("Segoe UI Emoji", 24)
    ICON = ("Segoe UI Emoji", 16)

# --- GESTOR DE DATOS ---
class DataManager:
    CONTACTS_FILE = "contacts.json"
    RECENTS_FILE = "recents.json"
    MESSAGES_FILE = "messages.json"

    def __init__(self):
        self.contacts = self._load_json(self.CONTACTS_FILE)
        self.recents = self._load_json(self.RECENTS_FILE)
        self.messages = self._load_json(self.MESSAGES_FILE) # Dict[number, List[Dict]]
        self.global_messages = [] # In-memory only for now

    def _load_json(self, filename):
        if not os.path.exists(filename): return {} if filename == self.CONTACTS_FILE else []
        try:
            with open(filename, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {} if filename == self.CONTACTS_FILE else []

    def _save_json(self, filename, data):
        try:
            with open(filename, 'w', encoding='utf-8') as f: json.dump(data, f, indent=2)
        except: pass

    def add_contact(self, name, number):
        self.contacts[number] = name
        self._save_json(self.CONTACTS_FILE, self.contacts)

    def get_name(self, number):
        return self.contacts.get(number, number)

    def add_recent(self, number, name, call_type): 
        entry = {"number": number, "name": name, "type": call_type, "time": datetime.now().strftime("%d/%m %H:%M")}
        self.recents.insert(0, entry)
        if len(self.recents) > 50: self.recents.pop()
        self.recents.insert(0, entry)
        if len(self.recents) > 50: self.recents.pop()
        self._save_json(self.RECENTS_FILE, self.recents)

    def add_message(self, number, text, direction):
        # direction: "in" or "out"
        if number not in self.messages: self.messages[number] = []
        entry = {
            "text": text,
            "direction": direction, # "in" | "out"
            "time": datetime.now().strftime("%H:%M")
        }
        self.messages[number].append(entry)
        self._save_json(self.MESSAGES_FILE, self.messages)
        return entry

    def add_global_message(self, sender, name, text):
        entry = {
            "sender": sender,
            "name": name,
            "text": text,
            "time": datetime.now().strftime("%H:%M")
        }
        self.global_messages.append(entry)
        if len(self.global_messages) > 100: self.global_messages.pop(0)
        return entry

# --- CLIENTE VOIP (BACKEND) ---
class VoIPClient:
    def __init__(self, server_host, server_port, number, name, ui_callback):
        self.server_host = server_host
        self.server_port = int(server_port)
        self.number = number
        self.name = name
        self.ui = ui_callback
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", 0))
        self.local_port = self.sock.getsockname()[1]
        
        self.running = True
        self.connected = False
        self.peer = None
        self.in_call = False
        
        self.audio_queue = Queue(maxsize=JITTER_BUFFER_SIZE * 2) 
        self.p = None
        self.stream_in = None
        self.stream_out = None
        
        self.input_device_index = -1
        self.output_device_index = -1
        self.input_gain = 1.0 
        
        self.audio_format = pyaudio.paInt16 if PYAUDIO_AVAILABLE else 8
        self._start_threads()

    def _start_threads(self):
        threading.Thread(target=self._listen, daemon=True).start()
        threading.Thread(target=self._heartbeat, daemon=True).start()

    def send(self, payload):
        try: self.sock.sendto(payload.encode(), (self.server_host, self.server_port))
        except: pass

    def _listen(self):
        while self.running:
            try:
                self.sock.settimeout(1.0)
                try: data, _ = self.sock.recvfrom(65535)
                except socket.timeout: continue
                msg = data.decode(errors="ignore").strip()
                if msg: self._process(msg)
            except: pass

    def _process(self, msg):
        if msg == "OK": pass
        elif msg == "PONG": self.ui.set_status(True)
        elif msg.startswith("AUDIO_FROM_B64:"):
            try: _, _, b64 = msg.split(":", 2)
            except: return
            self._audio_in(b64)
        else: self._handle_cmd(msg)

    def _handle_cmd(self, msg):
        parts = msg.split(":")
        cmd = parts[0]
        if cmd == "CALL_FROM": self._incoming(parts[1], parts[2] if len(parts)>2 else "")
        elif cmd == "ACCEPT_FROM": self._call_connected(parts[1])
        elif cmd == "RINGING_FROM": pass
        elif cmd in ("REJECT_FROM", "BUSY_FROM", "BYE_FROM"):
            self.ui.on_call_end()
        elif cmd in ("REJECT_FROM", "BUSY_FROM", "BYE_FROM"):
            self.ui.on_call_end()
            self._cleanup_call()
        elif cmd == "SMS_PRIVATE_FROM":
            # SMS_PRIVATE_FROM:source:name:msg
            if len(parts) >= 4:
                self.ui.on_private_sms(parts[1], parts[2], ":".join(parts[3:]))
        elif cmd == "SMS_GLOBAL_FROM":
            # SMS_GLOBAL_FROM:source:name:msg
            if len(parts) >= 4:
                self.ui.on_global_sms(parts[1], parts[2], ":".join(parts[3:]))

    def _heartbeat(self):
        while self.running:
            self.send(f"PING:{self.number}")
            time.sleep(10)

    def _register(self):
        self.send(f"REGISTER:{self.number}:{self.local_port}:{self.name}")

    def call(self, target):
        if self.in_call: return
        self.peer = target
        self.send(f"CALL:{target}:{self.number}")

    def accept(self, caller):
        self.send(f"ACCEPT:{caller}:{self.number}")
        self._call_connected(caller)

    def reject(self, caller):
        self.send(f"REJECT:{caller}:{self.number}")

    def hangup(self):
        if self.peer: self.send(f"BYE:{self.peer}:{self.number}")
        self._cleanup_call()
        
    def _incoming(self, caller, name):
        if self.in_call: return
        self.peer = caller
        self.ui.show_incoming(caller, name)

    def _call_connected(self, peer):
        self.in_call = True
        self.peer = peer
        self.ui.show_incall(peer)
        self._start_audio()

    def _cleanup_call(self):
        self.in_call = False
        self.peer = None
        self._stop_audio()
        self.ui.show_main()

    # AUDIO
    def _start_audio(self):
        if not PYAUDIO_AVAILABLE: return
        self.p = pyaudio.PyAudio()
        try:
            kw_in = {'format': self.audio_format, 'channels': 1, 'rate': AUDIO_RATE, 'input': True, 'frames_per_buffer': AUDIO_CHUNK, 'stream_callback': self._mic_cb}
            if self.input_device_index >= 0: kw_in['input_device_index'] = self.input_device_index
            self.stream_in = self.p.open(**kw_in)
            
            kw_out = {'format': self.audio_format, 'channels': 1, 'rate': AUDIO_RATE, 'output': True, 'frames_per_buffer': AUDIO_CHUNK}
            if self.output_device_index >= 0: kw_out['output_device_index'] = self.output_device_index
            self.stream_out = self.p.open(**kw_out)
            
            self.stream_in.start_stream()
            self.stream_out.start_stream()
            threading.Thread(target=self._spk_loop, daemon=True).start()
        except Exception as e:
            logger.error(f"Audio start error: {e}")

    def _stop_audio(self):
        if self.stream_in: self.stream_in.close()
        if self.stream_out: self.stream_out.close()
        if self.p: self.p.terminate()
        self.stream_in = None
        self.stream_out = None
        with self.audio_queue.mutex: self.audio_queue.queue.clear()

    def _mic_cb(self, in_data, frame_count, time_info, status):
        if not self.in_call: return (None, pyaudio.paContinue)
        try:
            b64 = base64.b64encode(in_data).decode()
            self.send(f"AUDIO_B64:{self.peer}:{self.number}:{b64}")
        except: pass
        return (None, pyaudio.paContinue)

    def _audio_in(self, b64):
        if not self.in_call: return
        try:
            if self.audio_queue.full(): self.audio_queue.get_nowait()
            self.audio_queue.put_nowait(base64.b64decode(b64))
        except: pass

    def _spk_loop(self):
        while self.in_call and self.stream_out:
            try:
                chunk = self.audio_queue.get(timeout=0.1)
                self.stream_out.write(chunk)
            except: pass
    
    def close(self):
        self.running = False
        self.hangup()
        self.send(f"UNREGISTER:{self.number}")

    def send_private_sms(self, target, msg):
        self.send(f"SMS_PRIVATE:{target}:{self.number}:{msg}")

    def send_global_sms(self, msg):
        self.send(f"SMS_GLOBAL:{self.number}:{msg}")


# --- INTERFAZ FLUIDA (CUSTOM FRAMES) ---
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Phone")
        self.geometry("380x700")
        self.configure(bg=UIColors.BG_MAIN)
        self.resizable(False, False)
        
        self.data = DataManager()
        self.client = None
        
        self.my_number = f"3{random.randint(100000000, 999999999)}"
        self.server_host = "147.135.213.72"
        self.server_port = 20159
        
        self._init_layout()
        self.after(200, self._connect)

    def _connect(self):
        if self.client: self.client.close()
        self.client = VoIPClient(self.server_host, self.server_port, self.my_number, "User", self)
        self.client._register()

    def _init_layout(self):
        # 1. Container
        self.content_area = tk.Frame(self, bg=UIColors.BG_MAIN)
        self.content_area.pack(expand=True, fill="both")
        
        self.current_frame = None
        
        # 2. Nav Bar
        self.nav_bar = tk.Frame(self, bg=UIColors.BG_CARD, height=60)
        self.nav_bar.pack(side="bottom", fill="x")
        self.nav_bar.pack_propagate(False)
        self._create_nav_btn("Teclado", "🔢", self._show_keypad)
        self._create_nav_btn("Contactos", "👥", self._show_contacts)
        self._create_nav_btn("Recientes", "🕒", self._show_recents)
        self._create_nav_btn("Mensajes", "💬", self._show_messages)
        
        # 3. HEADER (Status + Menu)
        self.header = tk.Frame(self, bg=UIColors.BG_MAIN, height=40)
        self.header.pack(side="top", fill="x", padx=15, pady=5, before=self.content_area)
        
        # Status
        self.status_lbl = tk.Label(self.header, text="...", fg=UIColors.TEXT_SEC, bg=UIColors.BG_MAIN, font=UIFonts.SMALL)
        self.status_lbl.pack(side="left")
        
        # MENU BUTTON (⋮)
        self.menu_btn = tk.Label(self.header, text="⋮", fg=UIColors.TEXT_MAIN, bg=UIColors.BG_MAIN, font=("Segoe UI", 18, "bold"), cursor="hand2")
        self.menu_btn.pack(side="right", padx=(10, 0))
        self.menu_btn.bind("<Button-1>", lambda e: self._show_settings())
        
        # ID
        tk.Label(self.header, text=f"ID: {self.my_number}", fg=UIColors.ACCENT, bg=UIColors.BG_MAIN, font=UIFonts.SMALL).pack(side="right")

        self._show_keypad()

    def _create_nav_btn(self, text, icon, cmd):
        btn_frame = tk.Frame(self.nav_bar, bg=UIColors.BG_CARD)
        btn_frame.pack(side="left", expand=True, fill="both")
        def on_click(e): cmd()
        lbl_icon = tk.Label(btn_frame, text=icon, font=UIFonts.ICON, bg=UIColors.BG_CARD, fg=UIColors.TEXT_SEC)
        lbl_icon.pack(expand=True)
        lbl_icon.bind("<Button-1>", on_click)
        btn_frame.bind("<Button-1>", on_click)

    def _switch_content(self, frame_class):
        if self.current_frame: self.current_frame.destroy()
        self.current_frame = frame_class(self.content_area, self)
        self.current_frame.pack(expand=True, fill="both")

    def _show_keypad(self): self._switch_content(KeypadView)
    def _show_contacts(self): self._switch_content(ContactsView)
    def _show_keypad(self): self._switch_content(KeypadView)
    def _show_contacts(self): self._switch_content(ContactsView)
    def _show_recents(self): self._switch_content(RecentsView)
    def _show_messages(self): self._switch_content(MessagesView)
    
    def open_private_chat(self, number):
        if self.current_frame: self.current_frame.destroy()
        self.current_frame = PrivateChatView(self.content_area, self, number)
        self.current_frame.pack(expand=True, fill="both")
    
    def on_private_sms(self, sender, name, msg):
        self.data.add_message(sender, msg, "in")
        if isinstance(self.current_frame, PrivateChatView) and self.current_frame.number == sender:
            self.current_frame.refresh()
        elif isinstance(self.current_frame, MessagesView):
            self.current_frame.refresh_list()
            
    def on_global_sms(self, sender, name, msg):
        self.data.add_global_message(sender, name, msg)
        if isinstance(self.current_frame, MessagesView):
            self.current_frame.refresh_global()
    
    def _show_settings(self):
        SettingsWindow(self)

    def set_status(self, connected):
        color = UIColors.SUCCESS if connected else UIColors.DANGER
        text = "4G LTE" if connected else "Sin Señal"
        self.status_lbl.config(text=text, fg=color)

    # CALL UI HANDLERS
    def show_incoming(self, caller, name): IncomingCallWindow(self, caller, name)
    def show_incall(self, peer): InCallWindow(self, peer)
    def show_main(self): pass
    def on_call_end(self): pass


# --- VISTAS ---
class KeypadView(tk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, bg=UIColors.BG_MAIN)
        self.app = app
        self.num_var = tk.StringVar()
        
        display = tk.Label(self, textvariable=self.num_var, font=UIFonts.H1, fg=UIColors.TEXT_MAIN, bg=UIColors.BG_MAIN)
        display.pack(pady=(40, 20))
        
        grid = tk.Frame(self, bg=UIColors.BG_MAIN)
        grid.pack(expand=True)
        keys = [['1',''],['2','ABC'],['3','DEF'],['4','GHI'],['5','JKL'],['6','MNO'],['7','PQRS'],['8','TUV'],['9','WXYZ'],['*',''],['0','+'],['#','']]
        r=0; c=0
        for k, sub in keys:
            self._key_btn(grid, k, sub, r, c)
            c+=1; 
            if c>2: c=0; r+=1
            
        actions = tk.Frame(self, bg=UIColors.BG_MAIN)
        actions.pack(fill="x", pady=20, padx=40)
        tk.Button(actions, text="📞", font=UIFonts.ICON_LG, bg=UIColors.SUCCESS, fg="white", bd=0, width=4, command=self._call).pack(side="top")
        tk.Button(actions, text="⌫", font=UIFonts.H3, bg=UIColors.BG_MAIN, fg=UIColors.TEXT_SEC, bd=0, command=self._backspace).place(relx=0.85, rely=0.5, anchor="center")

    def _key_btn(self, parent, text, sub, r, c):
        f = tk.Frame(parent, bg=UIColors.BG_CARD, width=80, height=80)
        f.grid_propagate(False)
        f.grid(row=r, column=c, padx=8, pady=8)
        def click(e): self.num_var.set(self.num_var.get() + text)
        l = tk.Label(f, text=text, font=UIFonts.H2, bg=UIColors.BG_CARD, fg=UIColors.TEXT_MAIN); l.pack(expand=True)
        l.bind("<Button-1>", click); f.bind("<Button-1>", click)

    def _backspace(self): self.num_var.set(self.num_var.get()[:-1])
    def _call(self):
        num = self.num_var.get()
        if num and self.app.client:
            self.app.data.add_recent(num, self.app.data.get_name(num), "outgoing")
            self.app.client.call(num)

class ContactsView(tk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, bg=UIColors.BG_MAIN)
        self.app = app
        h = tk.Frame(self, bg=UIColors.BG_MAIN)
        h.pack(fill="x", padx=20, pady=20)
        tk.Label(h, text="Contactos", font=UIFonts.H1, bg=UIColors.BG_MAIN, fg=UIColors.TEXT_MAIN).pack(side="left")
        tk.Button(h, text="+", font=UIFonts.H2, bg=UIColors.BG_MAIN, fg=UIColors.ACCENT, bd=0, command=self._add).pack(side="right")
        self.listbox = tk.Listbox(self, bg=UIColors.BG_MAIN, fg=UIColors.TEXT_MAIN, font=UIFonts.BODY, bd=0, highlightthickness=0, selectbackground=UIColors.BG_CARD)
        self.listbox.pack(fill="both", expand=True, padx=20); self.listbox.bind('<Double-1>', self._call_sel); self._refresh()
    def _refresh(self):
        self.listbox.delete(0, 'end')
        for n, name in self.app.data.contacts.items(): self.listbox.insert('end', f"{name}  -  {n}")
    def _add(self):
        name = simpledialog.askstring("Nuevo", "Nombre:")
        if name:
            num = simpledialog.askstring("Nuevo", "Número:")
            if num: self.app.data.add_contact(name, num); self._refresh()
    def _call_sel(self, e):
        sel = self.listbox.curselection()
        if sel:
            txt = self.listbox.get(sel[0]); num = txt.split(" - ")[-1]
            if self.app.client: self.app.client.call(num)

class RecentsView(tk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, bg=UIColors.BG_MAIN)
        self.app = app
        tk.Label(self, text="Recientes", font=UIFonts.H1, bg=UIColors.BG_MAIN, fg=UIColors.TEXT_MAIN, anchor="w").pack(fill="x", padx=20, pady=20)
        self.listbox = tk.Listbox(self, bg=UIColors.BG_MAIN, fg=UIColors.TEXT_MAIN, font=UIFonts.BODY, bd=0, highlightthickness=0, selectbackground=UIColors.BG_CARD)
        self.listbox.pack(fill="both", expand=True, padx=20); self.listbox.bind('<Double-1>', self._call_sel)
        for r in self.app.data.recents:
            icon = "➚" if r['type']=='outgoing' else "➘"
            self.listbox.insert('end', f"{icon}  {r['name'] or r['number']}   ({r['time']})")
    def _call_sel(self, e):
        sel = self.listbox.curselection()
        if sel:
            idx = sel[0]; num = self.app.data.recents[idx]['number']
            if self.app.client: self.app.client.call(num)

        if sel:
            idx = sel[0]; num = self.app.data.recents[idx]['number']
            if self.app.client: self.app.client.call(num)

class MessagesView(tk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, bg=UIColors.BG_MAIN)
        self.app = app
        self.mode = "global" # or "private"
        
        # Tabs
        tabs = tk.Frame(self, bg=UIColors.BG_CARD, height=50)
        tabs.pack(fill="x")
        self.btn_global = TkBtn(tabs, text="Global", cmd=lambda: self._set_mode("global"))
        self.btn_global.pack(side="left", expand=True, fill="both")
        self.btn_priv = TkBtn(tabs, text="Privados", cmd=lambda: self._set_mode("private"))
        self.btn_priv.pack(side="left", expand=True, fill="both")
        
        self.content = tk.Frame(self, bg=UIColors.BG_MAIN)
        self.content.pack(expand=True, fill="both", padx=10, pady=10)
        
        self._set_mode("global")

    def _set_mode(self, mode):
        self.mode = mode
        self.btn_global.config(bg=UIColors.ACCENT if mode=="global" else UIColors.BG_CARD, fg="white" if mode=="global" else UIColors.TEXT_SEC)
        self.btn_priv.config(bg=UIColors.ACCENT if mode=="private" else UIColors.BG_CARD, fg="white" if mode=="private" else UIColors.TEXT_SEC)
        
        for w in self.content.winfo_children(): w.destroy()
        
        if mode == "global":
            self._init_global()
        else:
            self._init_private_list()

    def _init_global(self):
        self.txt_log = tk.Text(self.content, bg=UIColors.BG_MAIN, fg=UIColors.TEXT_MAIN, font=UIFonts.BODY, bd=0, state="disabled")
        self.txt_log.pack(expand=True, fill="both")
        
        input_frame = tk.Frame(self.content, bg=UIColors.BG_Card if hasattr(UIColors, 'BG_Card') else UIColors.BG_CARD) # Fix typo fallback
        input_frame.pack(fill="x", pady=5)
        self.e_msg = tk.Entry(input_frame, bg=UIColors.BG_HOVER, fg="#FFF", font=UIFonts.BODY, bd=0)
        self.e_msg.pack(side="left", expand=True, fill="both", ipady=5, padx=5)
        self.e_msg.bind("<Return>", self._send_global)
        tk.Button(input_frame, text="➤", bg=UIColors.ACCENT, fg="white", bd=0, command=self._send_global).pack(side="right")
        self.refresh_global()

    def _send_global(self, e=None):
        msg = self.e_msg.get().strip()
        if msg and self.app.client:
            self.app.client.send_global_sms(msg)
            self.e_msg.delete(0, "end")
            # Optimistic update handled by server echo usually, but server ignores sender in broadcast.
            # So we add it locally.
            self.app.data.add_global_message(self.app.client.number, "Yo", msg)
            self.refresh_global()

    def refresh_global(self):
        if self.mode != "global": return
        self.txt_log.config(state="normal")
        self.txt_log.delete("1.0", "end")
        for m in self.app.data.global_messages:
            sender = "Yo" if m['sender'] == (self.app.client.number if self.app.client else "") else (m['name'] or m['sender'])
            self.txt_log.insert("end", f"[{m['time']}] {sender}: {m['text']}\n")
        self.txt_log.see("end")
        self.txt_log.config(state="disabled")

    def _init_private_list(self):
        h = tk.Frame(self.content, bg=UIColors.BG_MAIN)
        h.pack(fill="x", pady=5)
        tk.Button(h, text="+ Nuevo Mensaje", bg=UIColors.ACCENT, fg="white", bd=0, font=UIFonts.SMALL, command=self._new_priv).pack(fill="x")
        
        self.priv_list = tk.Listbox(self.content, bg=UIColors.BG_MAIN, fg=UIColors.TEXT_MAIN, font=UIFonts.BODY, bd=0, highlightthickness=0)
        self.priv_list.pack(expand=True, fill="both", pady=10)
        self.priv_list.bind("<Double-1>", self._open_priv)
        self.refresh_list()

    def refresh_list(self):
        if self.mode != "private": return
        self.priv_list.delete(0, "end")
        for num in self.app.data.messages:
            name = self.app.data.get_name(num)
            last = self.app.data.messages[num][-1]
            self.priv_list.insert("end", f"{name} ({num}) - {last['time']}")

    def _new_priv(self):
        num = simpledialog.askstring("Nuevo Mensaje", "Número destino:")
        if num: self.app.open_private_chat(num)
    
    def _open_priv(self, e):
        sel = self.priv_list.curselection()
        if sel:
            txt = self.priv_list.get(sel[0]) # "Name (Num) - Time"
            # Extract number simply by looking at keys order? Unsafe.
            # Better re-iterate or parse.
            # Lazy parse:
            num = list(self.app.data.messages.keys())[sel[0]]
            self.app.open_private_chat(num)

class PrivateChatView(tk.Frame):
    def __init__(self, parent, app, number):
        super().__init__(parent, bg=UIColors.BG_MAIN)
        self.app = app
        self.number = number # Target
        
        # Header
        h = tk.Frame(self, bg=UIColors.BG_CARD, height=40)
        h.pack(fill="x")
        tk.Button(h, text="<", bg=UIColors.BG_CARD, fg=UIColors.ACCENT, bd=0, font=UIFonts.H3, command=app._show_messages).pack(side="left")
        tk.Label(h, text=app.data.get_name(number), bg=UIColors.BG_CARD, fg=UIColors.TEXT_MAIN, font=UIFonts.H3).pack(side="left", padx=10)
        
        self.txt_log = tk.Text(self, bg=UIColors.BG_MAIN, fg=UIColors.TEXT_MAIN, font=UIFonts.BODY, bd=0, state="disabled")
        self.txt_log.pack(expand=True, fill="both", padx=10)
        
        input_frame = tk.Frame(self, bg=UIColors.BG_CARD)
        input_frame.pack(fill="x")
        self.e_msg = tk.Entry(input_frame, bg=UIColors.BG_HOVER, fg="#FFF", font=UIFonts.BODY, bd=0)
        self.e_msg.pack(side="left", expand=True, fill="both", ipady=5, padx=5, pady=5)
        self.e_msg.bind("<Return>", self._send)
        tk.Button(input_frame, text="Enviar", bg=UIColors.ACCENT, fg="white", bd=0, command=self._send).pack(side="right", padx=5)
        
        self.refresh()

    def _send(self, e=None):
        msg = self.e_msg.get().strip()
        if msg and self.app.client:
            self.app.client.send_private_sms(self.number, msg)
            self.app.data.add_message(self.number, msg, "out")
            self.e_msg.delete(0, "end")
            self.refresh()

    def refresh(self):
        self.txt_log.config(state="normal")
        self.txt_log.delete("1.0", "end")
        msgs = self.app.data.messages.get(self.number, [])
        for m in msgs:
            prefix = "Yo" if m['direction'] == "out" else self.app.data.get_name(self.number)
            self.txt_log.insert("end", f"[{m['time']}] {prefix}: {m['text']}\n")
        self.txt_log.see("end")
        self.txt_log.config(state="disabled")

# Helpers
class TkBtn(tk.Label): # Hack to make a flat label button
    def __init__(self, parent, text, cmd):
        super().__init__(parent, text=text, font=UIFonts.BODY, cursor="hand2")
        self.bind("<Button-1>", lambda e: cmd())

# --- POPUPS ---
class SettingsWindow(tk.Toplevel):
    def __init__(self, app):
        super().__init__(app)
        self.app = app
        self.title("Ajustes")
        self.geometry("340x500"); self.configure(bg=UIColors.BG_MAIN)
        self.geometry(f"+{app.winfo_x()+20}+{app.winfo_y()+100}")
        
        tk.Label(self, text="Ajustes", font=UIFonts.H2, bg=UIColors.BG_MAIN, fg=UIColors.TEXT_MAIN).pack(pady=20)
        
        f = tk.Frame(self, bg=UIColors.BG_MAIN, padx=20)
        f.pack(fill="x")
        
        # Name
        tk.Label(f, text="Nombre Visible:", bg=UIColors.BG_MAIN, fg=UIColors.TEXT_SEC, anchor="w").pack(fill="x")
        self.e_name = tk.Entry(f, bg=UIColors.BG_CARD, fg=UIColors.TEXT_MAIN, bd=0, font=UIFonts.BODY)
        self.e_name.insert(0,  app.client.name if app.client else "User")
        self.e_name.pack(fill="x", ipady=5, pady=(5, 20))
        
        # Devices
        tk.Label(f, text="Micrófono:", bg=UIColors.BG_MAIN, fg=UIColors.TEXT_SEC, anchor="w").pack(fill="x")
        self.cb_in = ttk.Combobox(f, state="readonly", values=self._get_devs(True)); 
        if self.cb_in['values']: self.cb_in.current(0)
        self.cb_in.pack(fill="x", pady=(5,20))

        tk.Label(f, text="Altavoz:", bg=UIColors.BG_MAIN, fg=UIColors.TEXT_SEC, anchor="w").pack(fill="x")
        self.cb_out = ttk.Combobox(f, state="readonly", values=self._get_devs(False)); 
        if self.cb_out['values']: self.cb_out.current(0)
        self.cb_out.pack(fill="x", pady=(5,20))
        
        tk.Button(self, text="Guardar", bg=UIColors.ACCENT, fg="white", font=UIFonts.H3, bd=0, command=self._save).pack(side="bottom", pady=30, padx=20, fill="x")

    def _get_devs(self, is_input):
        d=["Default"]
        if PYAUDIO_AVAILABLE:
            p = pyaudio.PyAudio()
            for i in range(p.get_device_count()):
                info = p.get_device_info_by_index(i)
                if (is_input and info['maxInputChannels']>0) or (not is_input and info['maxOutputChannels']>0):
                    d.append(f"{i}: {info['name']}")
            p.terminate()
        return d
        
    def _save(self):
        if self.app.client:
            self.app.client.name = self.e_name.get()
            def g(c): return int(c.get().split(":")[0]) if ":" in c.get() else -1
            self.app.client.input_device_index = g(self.cb_in)
            self.app.client.output_device_index = g(self.cb_out)
            self.app.client._register()
        self.destroy()

class IncomingCallWindow(tk.Toplevel):
    def __init__(self, app, caller, name):
        super().__init__(app)
        self.app = app
        self.configure(bg=UIColors.BG_MAIN); self.overrideredirect(True)
        self.geometry(f"300x500+{app.winfo_x()+40}+{app.winfo_y()+100}"); self.attributes('-topmost', True)
        tk.Label(self, text="Llamada Entrante", fg=UIColors.TEXT_SEC, bg=UIColors.BG_MAIN, font=UIFonts.BODY).pack(pady=40)
        tk.Label(self, text=name or caller, fg=UIColors.TEXT_MAIN, bg=UIColors.BG_MAIN, font=UIFonts.H2).pack()
        btns = tk.Frame(self, bg=UIColors.BG_MAIN); btns.pack(side="bottom", pady=40)
        tk.Button(btns, text="Rechazar", bg=UIColors.DANGER, fg="white", width=10, height=2, bd=0, command=lambda: [self.app.client.reject(caller), self.destroy()]).pack(side="left", padx=10)
        tk.Button(btns, text="Aceptar", bg=UIColors.SUCCESS, fg="white", width=10, height=2, bd=0, command=lambda: [self.app.client.accept(caller), self.destroy()]).pack(side="right", padx=10)
        if WINSOUND_AVAILABLE: threading.Thread(target=self._ring, daemon=True).start()
    def _ring(self):
        for _ in range(10): 
            try: 
                if not self.winfo_exists(): break
                winsound.PlaySound("SystemAsterisk", winsound.SND_ALIAS); time.sleep(1)
            except: break

class InCallWindow(tk.Toplevel):
    def __init__(self, app, peer):
        super().__init__(app)
        self.app = app; self.configure(bg=UIColors.BG_MAIN); self.overrideredirect(True)
        self.geometry(f"380x700+{app.winfo_x()}+{app.winfo_y()}")
        name = app.data.get_name(peer)
        tk.Label(self, text=name, fg=UIColors.TEXT_MAIN, bg=UIColors.BG_MAIN, font=UIFonts.H1).pack(pady=80)
        self.lbl = tk.Label(self, text="00:00", fg=UIColors.TEXT_SEC, bg=UIColors.BG_MAIN, font=UIFonts.H3); self.lbl.pack()
        tk.Button(self, text="COLGAR", bg=UIColors.DANGER, fg="white", font=UIFonts.H3, width=15, height=2, bd=0, command=lambda: [self.app.client.hangup(), self.destroy()]).pack(side="bottom", pady=60)
        self.s=0; self._tick()
    def _tick(self):
        if not self.winfo_exists(): return
        self.s+=1; m,s=divmod(self.s,60); self.lbl.config(text=f"{m:02d}:{s:02d}")
        self.after(1000, self._tick)

if __name__ == "__main__":
    app = App()
    app.mainloop()
