"""서버 실행 테스트"""
import sys
import subprocess

print("의존성 확인 중...")
try:
    import fastapi
    print(f"✓ FastAPI 설치됨: {fastapi.__version__}")
except ImportError as e:
    print(f"✗ FastAPI 설치 안됨: {e}")
    sys.exit(1)

try:
    import uvicorn
    print(f"✓ Uvicorn 설치됨: {uvicorn.__version__}")
except ImportError as e:
    print(f"✗ Uvicorn 설치 안됨: {e}")
    sys.exit(1)

try:
    from app import app
    print("✓ app.py 임포트 성공")
except Exception as e:
    print(f"✗ app.py 임포트 실패: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n서버 시작 중...")
print("서버 주소: http://localhost:8000")
print("API 문서: http://localhost:8000/docs")
print("\n서버를 중지하려면 Ctrl+C를 누르세요.\n")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")

