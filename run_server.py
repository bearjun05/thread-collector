"""서버 실행 스크립트 - 오류 확인용"""
import sys
import traceback

print("=" * 50)
print("Threads Collector API 서버 시작")
print("=" * 50)

try:
    print("\n1. 의존성 확인 중...")
    import fastapi
    import uvicorn
    print(f"   ✓ FastAPI: {fastapi.__version__}")
    print(f"   ✓ Uvicorn: {uvicorn.__version__}")
except ImportError as e:
    print(f"   ✗ 의존성 오류: {e}")
    traceback.print_exc()
    sys.exit(1)

try:
    print("\n2. app 모듈 임포트 중...")
    from app import app
    print("   ✓ app 모듈 임포트 성공")
except Exception as e:
    print(f"   ✗ app 모듈 임포트 실패: {e}")
    traceback.print_exc()
    sys.exit(1)

try:
    print("\n3. 서버 시작 중...")
    import os
    port = int(os.getenv("PORT", "8001"))
    print(f"   포트: {port}")
    print(f"   서버 주소: http://0.0.0.0:{port}")
    print(f"   로컬 주소: http://localhost:{port}")
    print(f"   API 문서: http://localhost:{port}/docs")
    print("\n" + "=" * 50)
    print("서버가 실행 중입니다. 중지하려면 Ctrl+C를 누르세요.")
    print("=" * 50 + "\n")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port,
        log_level="info"
    )
except KeyboardInterrupt:
    print("\n\n서버가 중지되었습니다.")
except Exception as e:
    print(f"\n   ✗ 서버 시작 실패: {e}")
    traceback.print_exc()
    sys.exit(1)

