FROM python:3.9-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ make curl && rm -rf /var/lib/apt/lists/*

# Instalar dependências Python
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir pysus==0.7.0
RUN pip install --no-cache-dir pandas==2.1.4
RUN pip install --no-cache-dir pyreaddbc
RUN pip install --no-cache-dir chardet
RUN pip install --no-cache-dir numpy==1.24.3
RUN pip install --no-cache-dir pyarrow==14.0.2
RUN pip install --no-cache-dir openpyxl

# Testar instalação
RUN python -c "import pysus; print('PySUS:', pysus.__version__)"
RUN python -c "import pyreaddbc; print('pyreaddbc: OK')"

WORKDIR /app
RUN mkdir -p /data
CMD ["python3"]
