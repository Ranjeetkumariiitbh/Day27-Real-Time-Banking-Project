#!/usr/bin/env python3
import socket
import sys
import time

HOST = sys.argv[1] if len(sys.argv) > 1 else "localhost"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9999
FILE = sys.argv[3] if len(sys.argv) > 3 else "data/sample_transactions.csv"

print(f"Connecting to {HOST}:{PORT} ...")
with socket.create_connection((HOST, PORT)) as sock:
    print("Connected. Sending transactions...")
    with open(FILE, "r", encoding="utf-8") as f:
        next(f, None)  # skip header
        for line in f:
            line = line.strip()
            if not line:
                continue
            print("SEND:", line)
            sock.sendall((line + "\n").encode("utf-8"))
            time.sleep(2)

print("Finished sending sample transactions.")
