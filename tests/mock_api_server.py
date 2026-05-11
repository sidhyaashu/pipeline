import http.server
import json
import os
import urllib.parse

PORT = 8080
DATA_DIR = os.getenv("DATA_DIR", "./tests/data")

class MockAccordHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed_url.query)
        
        filename = params.get("filename", [None])[0]
        
        if not filename:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing filename parameter")
            return

        print(f"[MOCK API] Request received for: {filename}")
        
        local_path = os.path.join(DATA_DIR, f"{filename}.txt")
        if not os.path.exists(local_path):
            local_path = os.path.join(DATA_DIR, f"{filename.lower()}.txt")

        if os.path.exists(local_path):
            try:
                with open(local_path, "r", encoding="utf-8") as f:
                    data = f.read()
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(f"Error reading file: {e}".encode())
        else:
            print(f"[MOCK API] File not found: {local_path}")
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"File not found in mock data directory")

def run_server():
    server_address = ("", PORT)
    httpd = http.server.HTTPServer(server_address, MockAccordHandler)
    print(f"🚀 Mock Accord API running on port {PORT}...")
    print(f"📂 Serving files from {DATA_DIR}")
    httpd.serve_forever()

if __name__ == "__main__":
    run_server()
