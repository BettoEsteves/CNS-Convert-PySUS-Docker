@echo off
REM ========================================
REM INSTALADOR AUTOMATICO SIA APAC
REM Sistema de Processamento CNS DATASUS
REM ========================================

echo.
echo ========================================
echo SIA APAC MEDICAMENTOS - INSTALADOR
echo ========================================
echo.

cd /d "%~dp0"

REM Verificar se Dockerfile existe
echo [1/4] Verificando Dockerfile...
if exist Dockerfile (
    echo      OK - Dockerfile encontrado
) else (
    echo      ERRO - Dockerfile nao encontrado
    echo      Execute este script na pasta do projeto
    pause
    exit /b 1
)

REM Verificar Python
echo.
echo [2/4] Verificando Python...
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo      ERRO - Python nao encontrado
    echo      Instale Python 3.9+: https://www.python.org/downloads/
    pause
    exit /b 1
) else (
    python --version
    echo      OK - Python encontrado
)

REM Verificar Docker
echo.
echo [3/4] Verificando Docker...
docker --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo      ERRO - Docker nao encontrado
    echo      Instale Docker Desktop: https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
) else (
    echo      OK - Docker encontrado
)

REM Construir imagem Docker
echo.
echo [4/4] Construindo imagem Docker pysus:local...
echo      Aguarde 2-5 minutos (download e instalacao)...
echo.

docker build -t pysus:local .

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo INSTALACAO CONCLUIDA COM SUCESSO!
    echo ========================================
    echo.
    echo Imagem Docker criada: pysus:local
    echo.
    echo Verificando imagem...
    docker images pysus:local
    echo.
    echo ========================================
    echo PROXIMOS PASSOS:
    echo ========================================
    echo.
    echo 1. Criar ambiente virtual Python:
    echo    python -m venv .venv_sia
    echo.
    echo 2. Ativar ambiente virtual:
    echo    .venv_sia\Scripts\activate
    echo.
    echo 3. Instalar dependencias Python:
    echo    pip install -r requirements.txt
    echo.
    echo 4. Executar o sistema:
    echo    python sia_v5_claude.py
    echo.
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ERRO ao construir imagem Docker
    echo ========================================
    echo.
    echo Possiveis causas:
    echo - Docker Desktop nao esta rodando
    echo - Sem conexao com internet
    echo - Falta de espaco em disco
    echo.
    echo Solucao:
    echo 1. Abra Docker Desktop
    echo 2. Aguarde inicializar
    echo 3. Execute install.bat novamente
    echo.
)

echo.
pause