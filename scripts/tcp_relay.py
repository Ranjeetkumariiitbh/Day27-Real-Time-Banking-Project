#!/usr/bin/env python3

import socket
import threading

HOST = "localhost"
PORT = 9999

spark_conn = None
spark_lock = threading.Lock()


def handle_client(conn, addr):
    global spark_conn

    print(f"Client connected: {addr}")

    try:
        # First connected client is Spark.
        with spark_lock:
            if spark_conn is None:
                spark_conn = conn
                print("Spark receiver connected.")
                spark_client = True
            else:
                spark_client = False

        if spark_client:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
        else:
            print("Transaction sender connected.")

            while True:
                data = conn.recv(4096)

                if not data:
                    break

                with spark_lock:
                    if spark_conn is not None:
                        spark_conn.sendall(data)
                        print("Forwarded:", data.decode("utf-8").strip())
                    else:
                        print("Spark is not connected yet.")

    except Exception as e:
        print(f"Client error {addr}: {e}")

    finally:
        with spark_lock:
            if conn is spark_conn:
                spark_conn = None
                print("Spark receiver disconnected.")

        conn.close()
        print(f"Client disconnected: {addr}")


def main():
    print(f"Starting TCP relay on {HOST}:{PORT}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((HOST, PORT))
    server.listen(5)

    print("Waiting for Spark and transaction sender...")

    while True:
        conn, addr = server.accept()

        thread = threading.Thread(
            target=handle_client,
            args=(conn, addr),
            daemon=True
        )

        thread.start()


if __name__ == "__main__":
    main()
