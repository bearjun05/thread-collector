"""서버 상태 확인"""
import socket
import sys

def check_port(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"오류: {e}")
        return False

if __name__ == "__main__":
    host = "localhost"
    port = 8000
    
    print(f"포트 {port} 확인 중...")
    if check_port(host, port):
        print(f"✓ 서버가 {host}:{port}에서 실행 중입니다!")
        print(f"  API 문서: http://{host}:{port}/docs")
        sys.exit(0)
    else:
        print(f"✗ 서버가 {host}:{port}에서 실행되지 않았습니다.")
        print("\n서버를 실행하려면 다음 명령어를 사용하세요:")
        print("  uv run uvicorn app:app --host 0.0.0.0 --port 8000")
        print("  또는")
        print("  python app.py")
        sys.exit(1)

