import pandas as pd
import numpy as np
import re
from pathlib import Path
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from ftplib import FTP
import tempfile
import shutil
import threading
import struct
import io
from datetime import datetime
import traceback
import zlib
import gzip

print("🚀 Sistema SIA APAC Medicamentos - Carregando...")

base_path = r"E:\Projetos\SIA\dados"
os.makedirs(base_path, exist_ok=True)

ESTADOS = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
    'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo',
    'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
    'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
    'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins'
}

class DBCConverter:
    """Conversor de arquivos .dbc usando Docker PySUS"""

    @staticmethod
    def read_dbc(dbc_file, encoding='latin-1'):
        """
        Lê arquivo .dbc e retorna DataFrame usando Docker com PySUS
        DBC do DATASUS precisa de ambiente Linux com PySUS
        """
        try:
            print(f"🔧 Convertendo {Path(dbc_file).name}...")

            # Usar Docker com PySUS
            return DBCConverter._read_dbc_with_docker(dbc_file, encoding)

        except Exception as e:
            print(f"❌ Erro na conversão DBC: {e}")
            traceback.print_exc()
            return pd.DataFrame()

    @staticmethod
    def _read_dbc_with_docker(dbc_file, encoding='latin-1'):
        """Usa Docker com PySUS para converter DBC"""
        try:
            import subprocess

            dbc_path = Path(dbc_file).absolute()
            dados_dir = Path(base_path).absolute()

            # Garantir que o diretório dados existe
            dados_dir.mkdir(parents=True, exist_ok=True)

            # Nome do arquivo de saída
            output_name = dbc_path.stem + "_converted.parquet"
            output_path = dados_dir / output_name

            # Script Python para rodar no Docker
            script = f'''
import pyreaddbc
import pandas as pd
import sys

def converter_cns_para_numeros(valor):
    """
    Converte caracteres Latin-1 especiais para dígitos.
    DBC usa codificação especial: 0x7B-0x84 representam dígitos 0-9
    Baseado na tabela: 0x7B=0, 0x7C=1, 0x7D=2, ..., 0x84=9
    """
    if pd.isna(valor) or valor == '':
        return ''

    # Mapeamento de bytes Latin-1 para dígitos (comum em arquivos DBC)
    # Baseado na análise: {{{{ aparece muito, sugerindo que {{ = 0
    mapa = {{
        chr(0x7B): '0',  # {{
        chr(0x7C): '1',  # |
        chr(0x7D): '2',  # }}
        chr(0x7E): '3',  # ~
        chr(0x7F): '4',  # DEL
        chr(0x80): '5',  # €
        chr(0x81): '6',  #
        chr(0x82): '7',  # ‚
        chr(0x83): '8',  # ƒ
        chr(0x84): '9',  # „
    }}

    resultado = ''
    for char in str(valor):
        if char in mapa:
            resultado += mapa[char]
        elif char.isdigit():
            resultado += char

    return resultado

try:
    print("📖 Lendo DBC com encoding {encoding}...")
    df = pyreaddbc.read_dbc("/data/{dbc_path.name}", encoding="{encoding}")

    if df is not None and len(df) > 0:
        print(f"✅ Lido: {{len(df):,}} registros, {{len(df.columns)}} colunas")

        # CONVERTER AP_CNSPCN para números legíveis
        if 'AP_CNSPCN' in df.columns:
            print("🔧 Convertendo AP_CNSPCN de Latin-1 especial para números...")
            df['AP_CNSPCN'] = df['AP_CNSPCN'].apply(converter_cns_para_numeros)
            print(f"✅ Amostra AP_CNSPCN convertido: {{df['AP_CNSPCN'].head(5).tolist()}}")

        # Salvar como parquet
        df.to_parquet("/data/{output_name}", index=False)
        print(f"💾 Salvo: /data/{output_name}")
    else:
        print("❌ DataFrame vazio")
        sys.exit(1)

except Exception as e:
    print(f"❌ Erro: {{e}}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
'''

            # Salvar script temporário
            script_path = dados_dir / 'convert_dbc.py'
            script_path.write_text(script, encoding='utf-8')

            print("   🐳 Usando Docker com PySUS...")

            # Comando Docker
            cmd = [
                'docker', 'run', '--rm',
                '-v', f'{str(dados_dir)}:/data',
                'pysus:local',
                'python', '/data/convert_dbc.py'
            ]

            # Executar
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=300
            )

            # Mostrar saída
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    print(f"   {line}")

            if result.returncode != 0:
                print(f"   ❌ Docker falhou (código {result.returncode})")
                if result.stderr:
                    print(f"   Erro: {result.stderr}")
                return pd.DataFrame()

            # Ler o parquet gerado
            if output_path.exists():
                df = pd.read_parquet(output_path)
                print(f"   ✅ Convertido: {len(df):,} registros, {len(df.columns)} colunas")

                # Limpar arquivos temporários
                try:
                    script_path.unlink()
                    output_path.unlink()
                except:
                    pass

                return df
            else:
                print(f"   ❌ Arquivo parquet não foi gerado")
                return pd.DataFrame()

        except subprocess.TimeoutExpired:
            print("   ❌ Timeout na conversão Docker")
            return pd.DataFrame()
        except Exception as e:
            print(f"   ❌ Erro Docker: {e}")
            traceback.print_exc()
            return pd.DataFrame()

    @staticmethod
    def _parse_dbf_from_file(dbc_file, encoding):
        """Método alternativo caso pyreaddbc não funcione"""
        try:
            with open(dbc_file, 'rb') as f:
                compressed_data = f.read()

            if len(compressed_data) < 32:
                raise ValueError("Arquivo muito pequeno")

            # Tentar descomprimir com zlib
            print("   🗜️  Tentando descompressão manual...")
            dbf_data = None

            for offset in [0, 4, 8, 16, 32]:
                try:
                    dbf_data = zlib.decompress(compressed_data[offset:], -zlib.MAX_WBITS)
                    print(f"   ✅ Descomprimido com offset {offset}")
                    break
                except:
                    continue

            if dbf_data:
                return DBCConverter._parse_dbf(dbf_data, encoding)
            else:
                print("   ❌ Falha na descompressão")
                return pd.DataFrame()

        except Exception as e:
            print(f"   ❌ Erro: {e}")
            return pd.DataFrame()

    @staticmethod
    def _parse_dbf(dbf_data, encoding='cp850'):
        """Parse arquivo DBF (descomprimido)"""
        try:
            if len(dbf_data) < 32:
                return pd.DataFrame()
            
            # Ler cabeçalho DBF
            header = dbf_data[:32]
            
            # Verificar assinatura DBF
            dbf_type = header[0]
            if dbf_type not in [0x03, 0x83, 0x8B, 0xF5]:  # Tipos válidos DBF
                print(f"   ⚠️  Tipo DBF não reconhecido: 0x{dbf_type:02X}")
                return DBCConverter._parse_as_text(dbf_data, encoding)
            
            # Extrair informações do cabeçalho
            record_count = struct.unpack('<I', header[4:8])[0]
            header_length = struct.unpack('<H', header[8:10])[0]
            record_length = struct.unpack('<H', header[10:12])[0]
            
            print(f"   📊 DBF: {record_count:,} registros, {record_length} bytes/registro")
            
            # Calcular número de campos
            field_desc_length = header_length - 32 - 1  # -1 para o terminador 0x0D
            num_fields = field_desc_length // 32
            
            print(f"   📋 Campos detectados: {num_fields}")
            
            # TENTAR MÚLTIPLOS ENCODINGS
            encodings_to_try = ['cp850', 'cp437', 'latin-1', 'iso-8859-1', 'windows-1252']
            best_encoding = encoding
            
            # Ler descrição dos campos
            fields = []
            pos = 32
            
            for i in range(num_fields):
                field_desc = dbf_data[pos:pos+32]
                
                # Nome do campo (primeiros 11 bytes, zero-terminated)
                # Tentar diferentes encodings para nome do campo
                field_name = None
                for enc in encodings_to_try:
                    try:
                        name = field_desc[:11].split(b'\x00')[0].decode(enc, errors='strict').strip()
                        if name and name.isprintable():
                            field_name = name
                            best_encoding = enc
                            break
                    except:
                        continue
                
                if not field_name:
                    # Fallback: usar cp850 com ignorar erros
                    field_name = field_desc[:11].split(b'\x00')[0].decode('cp850', errors='ignore').strip()
                
                # Tipo do campo
                field_type = chr(field_desc[11])
                
                # Tamanho do campo
                field_length = field_desc[16]
                
                if field_name:  # Ignorar campos vazios
                    fields.append({
                        'name': field_name if field_name else f'FIELD_{i}',
                        'type': field_type,
                        'length': field_length
                    })
                
                pos += 32
            
            if not fields:
                print("   ⚠️  Nenhum campo encontrado")
                return DBCConverter._parse_as_text(dbf_data, encoding)
            
            print(f"   ✅ Campos válidos: {len(fields)}")
            print(f"   🔤 Encoding detectado: {best_encoding}")
            
            for i, field in enumerate(fields[:10]):  # Mostrar primeiros 10
                print(f"      {i+1}. {field['name']} ({field['type']}, {field['length']})")
            
            # Ler registros COM O MELHOR ENCODING
            data_start = header_length
            records = []
            
            for rec_num in range(record_count):
                rec_start = data_start + (rec_num * record_length)
                rec_end = rec_start + record_length
                
                if rec_end > len(dbf_data):
                    break
                
                record_data = dbf_data[rec_start:rec_end]
                
                # Primeiro byte: flag de deletado
                if record_data[0] == 0x2A:  # Deletado
                    continue
                
                # Extrair valores dos campos
                row = []
                field_pos = 1  # Pular flag
                
                for field in fields:
                    field_value = record_data[field_pos:field_pos + field['length']]

                    try:
                        # Decodificar com encoding detectado
                        # Para arquivos DATASUS, latin-1 é o padrão
                        value_str = field_value.decode(best_encoding, errors='replace').strip()

                        # Se o campo contém apenas dígitos e espaços (campo numérico)
                        # preservar todos os dígitos sem limpeza adicional
                        if field['type'] in ['N', 'C']:  # Numeric ou Character
                            # Preservar dígitos e espaços, remover apenas null bytes
                            value_str = value_str.replace('\x00', '').strip()
                        else:
                            # Limpar caracteres de controle para outros tipos
                            value_str = ''.join(char for char in value_str if char.isprintable() or char in ['\n', '\t'])

                        row.append(value_str)
                    except:
                        row.append('')

                    field_pos += field['length']
                
                if any(row):  # Pelo menos um campo não vazio
                    records.append(row)
            
            if not records:
                print("   ⚠️  Nenhum registro válido encontrado")
                return pd.DataFrame()
            
            # Criar DataFrame
            column_names = [field['name'] for field in fields]
            df = pd.DataFrame(records, columns=column_names)
            
            # Limpar dados
            for col in df.columns:
                # Remover caracteres nulos e espaços extras
                df[col] = df[col].astype(str).str.replace('\x00', '').str.strip()
                # Substituir strings vazias por None
                df[col] = df[col].replace('', None)
                df[col] = df[col].replace('nan', None)
            
            print(f"   ✅ Conversão: {len(df):,} registros, {len(df.columns)} colunas")
            
            # Mostrar amostra
            print("\n   📋 Primeiras colunas:")
            for col in df.columns[:5]:
                non_null = df[col].notna().sum()
                sample = df[col].dropna().head(3).tolist()
                print(f"      • {col}: {non_null:,} valores | Ex: {sample}")
            
            return df
            
        except Exception as e:
            print(f"   ❌ Erro no parse DBF: {e}")
            traceback.print_exc()
            return DBCConverter._parse_as_text(dbf_data, encoding)

    @staticmethod
    def _split_fixed_width(text, widths=None):
        """Divide texto em campos de largura fixa"""
        if not widths:
            # Tentar detectar larguras automaticamente
            # Procurar por sequências de espaços como separadores
            fields = []
            current = ''
            in_field = False
            
            for char in text:
                if char != ' ':
                    current += char
                    in_field = True
                elif in_field:
                    fields.append(current)
                    current = ''
                    in_field = False
            
            if current:
                fields.append(current)
            
            return fields
        
        # Usar larguras pré-definidas
        fields = []
        pos = 0
        for width in widths:
            if pos + width <= len(text):
                fields.append(text[pos:pos+width].strip())
                pos += width
            else:
                fields.append('')
        
        return fields

    @staticmethod
    def _identify_columns(df):
        """Tenta identificar nomes reais das colunas"""
        # Padrões comuns no SIA
        column_patterns = {
            'AP_CNSPCN': ['CNS', 'CNSPCN', 'AP_CNS'],
            'AP_MUNPCN': ['MUN', 'MUNIC', 'MUNPCN', 'COD_MUN'],
            'AP_CIDPRI': ['CID', 'DIAG', 'CIDPRI'],
            'AP_PRIPAL': ['PROC', 'PRIPAL', 'PROCED'],
            'AP_VL_AP': ['VALOR', 'VL_AP', 'VALOR_AP'],
            'AP_UFMUN': ['UF', 'UFMUN'],
            'AP_CMP': ['CMP', 'COMPET'],
            'AP_NUIDADE': ['IDADE', 'NUIDADE'],
            'AP_SEXO': ['SEXO'],
            'AP_RACACOR': ['RACA', 'RACACOR'],
            'AP_DTINIC': ['DTINIC', 'DATA_INI'],
            'AP_DTFIM': ['DTFIM', 'DATA_FIM'],
        }
        
        # Verificar primeira linha para ver se é cabeçalho
        first_row = df.iloc[0].astype(str).str.upper().tolist()
        header_like = any(any(pattern in cell for pattern in ['AP_', 'CNS', 'MUN', 'CID']) 
                         for cell in first_row)
        
        if header_like:
            # Usar primeira linha como cabeçalho
            new_columns = first_row
            df = df.iloc[1:].reset_index(drop=True)
            
            # Renomear colunas
            if len(new_columns) == len(df.columns):
                df.columns = new_columns
                print("✅ Usando primeira linha como cabeçalho")
        
        # Renomear colunas baseado em padrões
        for i, col in enumerate(df.columns):
            col_upper = str(col).upper()
            for std_name, patterns in column_patterns.items():
                for pattern in patterns:
                    if pattern.upper() in col_upper:
                        df = df.rename(columns={col: std_name})
                        print(f"   🔄 Renomeada coluna {i}: {col} -> {std_name}")
                        break
        
        return df

    @staticmethod
    def _parse_as_text(data, encoding):
        """Parsing alternativo como texto puro"""
        print("📝 Usando parsing textual...")
        
        try:
            # Decodificar todo o conteúdo
            text = data.decode(encoding, errors='ignore')
            
            # Procurar por linhas estruturadas
            lines = text.splitlines()
            
            # Filtrar linhas vazias
            lines = [line.strip() for line in lines if line.strip()]
            
            if len(lines) < 2:
                return pd.DataFrame()
            
            # Analisar estrutura
            possible_delimiters = [';', '|', '\t', ',']
            best_delimiter = None
            best_field_count = 0
            
            for delim in possible_delimiters:
                sample_lines = lines[:10]  # Analisar primeiras 10 linhas
                field_counts = [len(line.split(delim)) for line in sample_lines]
                avg_fields = np.mean(field_counts)
                
                if avg_fields > best_field_count and avg_fields > 1:
                    best_field_count = avg_fields
                    best_delimiter = delim
            
            if best_delimiter:
                print(f"✅ Delimitador detectado: '{best_delimiter}' ({best_field_count:.1f} campos)")
                
                # Separar campos
                all_fields = []
                for line in lines:
                    fields = line.split(best_delimiter)
                    fields = [field.strip() for field in fields]
                    if len(fields) > 1:  # Pelo menos 2 campos
                        all_fields.append(fields)
                
                if all_fields:
                    # Encontrar número máximo de campos
                    max_fields = max(len(fields) for fields in all_fields)
                    
                    # Padronizar número de campos
                    for fields in all_fields:
                        while len(fields) < max_fields:
                            fields.append('')
                    
                    # Criar DataFrame
                    columns = [f'FIELD_{i:03d}' for i in range(max_fields)]
                    df = pd.DataFrame(all_fields, columns=columns)
                    
                    # Tentar identificar cabeçalho
                    df = DBCConverter._identify_columns(df)
                    
                    print(f"✅ Parsing textual: {len(df):,} registros")
                    return df
            
            # Último recurso: criar DataFrame com uma coluna
            df = pd.DataFrame({'CONTEUDO': lines})
            print(f"⚠️  Criado DataFrame básico: {len(df):,} linhas")
            return df
            
        except Exception as e:
            print(f"❌ Erro no parsing textual: {e}")
            return pd.DataFrame()


class SIADownloader:
    """Gerencia download do DATASUS e conversão"""

    def __init__(self):
        self.ftp_host = "ftp.datasus.gov.br"
        self.ftp_path = "/dissemin/publicos/SIASUS/200801_/Dados/"

    def download_arquivo(self, grupo, uf, ano, mes):
        """Baixa arquivo DBC do DATASUS para o diretório dados"""
        filename = f"{grupo}{uf}{str(ano)[2:]}{str(mes).zfill(2)}.dbc"

        print(f"🌐 Baixando: {filename}")

        try:
            ftp = FTP(self.ftp_host, timeout=120)
            ftp.login()
            ftp.cwd(self.ftp_path)

            # Salvar no diretório dados (para usar com Docker)
            dados_dir = Path(base_path)
            dados_dir.mkdir(parents=True, exist_ok=True)
            dbc_path = dados_dir / filename

            # Baixar
            with open(dbc_path, 'wb') as f:
                ftp.retrbinary(f"RETR {filename}", f.write)

            ftp.quit()

            # Verificar se o arquivo foi baixado
            if dbc_path.exists() and dbc_path.stat().st_size > 0:
                size_mb = dbc_path.stat().st_size / (1024 * 1024)
                print(f"✅ Baixado: {filename} ({size_mb:.2f} MB)")
                return str(dbc_path)
            else:
                print(f"❌ Arquivo vazio: {filename}")
                return None

        except Exception as e:
            print(f"❌ Erro FTP: {e}")
            return None

    def processar_estado(self, uf, ano, grupo="AM", meses=None):
        """Processa todos os meses de um estado"""
        if meses is None:
            meses = [1]  # Apenas mês 1 para teste
        
        print(f"\n🚀 Processando {uf}-{ano}")
        print("=" * 60)
        
        dataframes = []
        
        for mes in meses:
            print(f"\n📅 Mês {mes:02d}")
            
            # 1. Download
            dbc_path = self.download_arquivo(grupo, uf, ano, mes)
            if not dbc_path:
                print("⏭️  Download falhou, pulando...")
                continue
            
            # 2. Converter DBC com encoding correto (latin-1 = ISO-8859-1 padrão DATASUS)
            df = DBCConverter.read_dbc(dbc_path, encoding='latin-1')
            
            if df is not None and len(df) > 0:
                # Adicionar metadados
                df['UF'] = uf
                df['ANO'] = ano
                df['MES'] = mes
                df['GRUPO'] = grupo
                
                dataframes.append(df)
                print(f"✅ Convertido: {len(df):,} registros")
            else:
                print(f"❌ Conversão falhou ou dados vazios")
            
            # Limpar arquivo temporário
            try:
                os.unlink(dbc_path)
            except:
                pass
        
        # Consolidar dados
        if dataframes:
            print(f"\n📊 Consolidando dados...")
            df_final = pd.concat(dataframes, ignore_index=True, sort=False)
            
            # Salvar
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = Path(base_path) / f"SIA_{uf}_{ano}_{timestamp}.parquet"
            
            df_final.to_parquet(output_file, index=False)
            
            print(f"\n🎉 PROCESSAMENTO CONCLUÍDO!")
            print(f"📊 Total registros: {len(df_final):,}")
            print(f"📋 Total colunas: {len(df_final.columns)}")
            print(f"💾 Arquivo salvo: {output_file}")
            
            return df_final, len(df_final)
        else:
            print(f"\n❌ Nenhum dado processado para {uf}-{ano}")
            return None, 0


def carregar_arquivo(caminho):
    """Carrega arquivo em vários formatos"""
    try:
        caminho = Path(caminho)
        if not caminho.exists():
            return None, "Arquivo não encontrado"

        extensao = caminho.suffix.lower()
        print(f"📂 Carregando: {caminho.name}")
        
        if extensao == '.parquet':
            df = pd.read_parquet(caminho)
        elif extensao == '.csv':
            # Tentar diferentes encodings
            encodings = ['latin-1', 'iso-8859-1', 'cp1252', 'utf-8']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(caminho, encoding=encoding, sep=';', 
                                   low_memory=False, on_bad_lines='skip')
                    print(f"✅ Encoding: {encoding}")
                    break
                except:
                    try:
                        df = pd.read_csv(caminho, encoding=encoding, sep=',',
                                       low_memory=False, on_bad_lines='skip')
                        print(f"✅ Encoding: {encoding} (sep=',')")
                        break
                    except:
                        continue
            
            if df is None:
                return None, "Não foi possível ler o CSV"
                
        elif extensao == '.dbc':
            # Converter DBC com encoding correto
            df = DBCConverter.read_dbc(str(caminho), encoding='latin-1')
            if df is None or len(df) == 0:
                return None, "Não foi possível converter o DBC"
        else:
            return None, f"Formato não suportado: {extensao}"
        
        print(f"📊 Carregado: {len(df):,} registros, {len(df.columns)} colunas")
        return df, None
        
    except Exception as e:
        return None, f"Erro ao carregar: {str(e)}"


def validar_cns(cns):
    """
    Valida CNS conforme algoritmo e-SUS Bridge UFSC.
    Referência: https://integracao.esusaps.bridge.ufsc.tech/v211/docs/algoritmo_CNS.html

    Args:
        cns (str): CNS com 15 dígitos

    Returns:
        tuple: (bool válido, str tipo) onde tipo pode ser 'definitivo', 'provisorio' ou 'invalido'
    """
    # Limpar e validar formato
    if not cns or not isinstance(cns, str):
        return False, 'invalido'

    cns_limpo = ''.join(c for c in str(cns) if c.isdigit())

    if len(cns_limpo) != 15:
        return False, 'invalido'

    # CNS Definitivo: inicia com 1 ou 2
    primeiro_digito = cns_limpo[0]

    if primeiro_digito in ['1', '2']:
        # Algoritmo para CNS definitivo
        pis = cns_limpo[:11]
        soma = sum(int(pis[i]) * (15 - i) for i in range(11))
        resto = soma % 11
        dv = 11 - resto

        if dv == 11:
            dv = 0
            cns_calculado = pis + "000" + str(dv)
        elif dv == 10:
            soma += 2
            resto = soma % 11
            dv = 11 - resto
            cns_calculado = pis + "001" + str(dv)
        else:
            cns_calculado = pis + "000" + str(dv)

        valido = (cns_limpo == cns_calculado)
        return valido, 'definitivo' if valido else 'invalido'

    # CNS Provisório: inicia com 7, 8 ou 9
    elif primeiro_digito in ['7', '8', '9']:
        # Algoritmo para CNS provisório
        soma = sum(int(cns_limpo[i]) * (15 - i) for i in range(15))
        resto = soma % 11
        valido = (resto == 0)
        return valido, 'provisorio' if valido else 'invalido'

    # Primeiro dígito inválido
    return False, 'invalido'


def processar_cns(df):
    """Processa CNS e cria ID único"""
    try:
        print("🔍 Processando CNS...")

        df_proc = df.copy()
        
        # DIAGNÓSTICO
        print(f"📋 Colunas disponíveis ({len(df.columns)}):")
        for i, col in enumerate(df.columns[:20]):
            print(f"  {i+1:2d}. {col}")
        
        # Procurar coluna CNS
        coluna_cns = None
        for col in df_proc.columns:
            col_upper = str(col).upper()
            if any(pattern in col_upper for pattern in ['CNS', 'CNSPCN', 'AP_CNS']):
                coluna_cns = col
                break
        
        if not coluna_cns:
            return None, "Coluna CNS não encontrada"
        
        # Procurar coluna município
        coluna_mun = None
        for col in df_proc.columns:
            col_upper = str(col).upper()
            if any(pattern in col_upper for pattern in ['MUN', 'MUNIC', 'COD_MUN']):
                coluna_mun = col
                break
        
        if not coluna_mun:
            coluna_mun = 'MUN_DUMMY'
            df_proc[coluna_mun] = '000000'
        
        print(f"✅ CNS: {coluna_cns}, Município: {coluna_mun}")
        
        # LIMPAR CNS
        df_proc['CNS_LIMPO'] = (
            df_proc[coluna_cns]
            .astype(str)
            .fillna('')
            .str.replace(r'[^0-9]', '', regex=True)
        )
        
        # PADRONIZAR CNS (15 dígitos) - Preserva zeros à esquerda
        def padronizar_cns(cns):
            """
            Padroniza CNS para 15 dígitos preservando zeros à esquerda.
            CNS deve ter exatamente 15 dígitos conforme especificação e-SUS.
            """
            if not cns or cns == '':
                return 'SEM_CNS'

            # Remover apenas espaços, manter números
            cns = cns.strip()

            # Se for apenas zeros, considerar como inválido
            if cns == '0' * len(cns):
                return '000000000000000'

            # Preencher com zeros à esquerda se necessário
            if len(cns) < 15:
                return cns.zfill(15)
            elif len(cns) > 15:
                # Truncar se maior (manter os primeiros 15)
                return cns[:15]
            else:
                return cns
        
        df_proc['CNS_PADRONIZADO'] = df_proc['CNS_LIMPO'].apply(padronizar_cns)

        # VALIDAR CNS
        print("🔍 Validando CNS...")
        validacao = df_proc['CNS_PADRONIZADO'].apply(
            lambda x: validar_cns(x) if x != 'SEM_CNS' and x != '000000000000000' else (False, 'sem_cns')
        )

        df_proc['CNS_VALIDO'] = validacao.apply(lambda x: x[0])
        df_proc['CNS_TIPO'] = validacao.apply(lambda x: x[1])

        # PROCESSAR MUNICÍPIO
        df_proc['MUN_CODIGO'] = (
            df_proc[coluna_mun]
            .astype(str)
            .fillna('000000')
            .str.replace(r'[^0-9]', '', regex=True)
            .str[:6]
            .apply(lambda x: x.zfill(6) if x.isdigit() else '000000')
        )
        
        # CRIAR ID ÚNICO
        df_proc['ID_PACIENTE'] = df_proc['MUN_CODIGO'] + "_" + df_proc['CNS_PADRONIZADO']
        
        # LIMPAR COLUNAS TEMPORÁRIAS
        df_proc = df_proc.drop(['CNS_LIMPO', 'MUN_CODIGO'], axis=1, errors='ignore')
        if coluna_mun == 'MUN_DUMMY':
            df_proc = df_proc.drop(['MUN_DUMMY'], axis=1, errors='ignore')
        
        # ESTATÍSTICAS
        total = len(df_proc)
        unicos = df_proc['ID_PACIENTE'].nunique()
        sem_cns = (df_proc['CNS_PADRONIZADO'] == 'SEM_CNS').sum()

        # Estatísticas de validação
        cns_validos = df_proc['CNS_VALIDO'].sum()
        cns_definitivos = (df_proc['CNS_TIPO'] == 'definitivo').sum()
        cns_provisorios = (df_proc['CNS_TIPO'] == 'provisorio').sum()
        cns_invalidos = (df_proc['CNS_TIPO'] == 'invalido').sum()

        print(f"\n📊 ESTATÍSTICAS:")
        print(f"   📈 Registros: {total:,}")
        print(f"   👥 Pacientes únicos: {unicos:,}")
        print(f"   ✅ Com CNS: {total - sem_cns:,}")
        print(f"   ❌ Sem CNS: {sem_cns:,}")
        print(f"\n🔍 VALIDAÇÃO CNS:")
        print(f"   ✅ Válidos: {cns_validos:,}")
        print(f"   📗 Definitivos: {cns_definitivos:,}")
        print(f"   📙 Provisórios: {cns_provisorios:,}")
        print(f"   ❌ Inválidos: {cns_invalidos:,}")
        
        return df_proc, None
        
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
        traceback.print_exc()
        return None, f"Erro: {str(e)}"


def exportar_dados(df, caminho, formato):
    """Exporta dados para arquivo"""
    try:
        caminho = Path(caminho)
        caminho.parent.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if formato == 'CSV':
            arquivo = caminho.with_name(f"{caminho.stem}_{timestamp}.csv")
            # Exportar com UTF-8 BOM para garantir compatibilidade com Excel
            df.to_csv(arquivo, index=False, sep=';', encoding='utf-8-sig', lineterminator='\n')
            print(f"   💾 Arquivo CSV salvo com encoding UTF-8-BOM")
        elif formato == 'Parquet':
            arquivo = caminho.with_name(f"{caminho.stem}_{timestamp}.parquet")
            df.to_parquet(arquivo, index=False, engine='pyarrow', compression='snappy')
            print(f"   💾 Arquivo Parquet salvo com compressão snappy")
        elif formato == 'Excel':
            arquivo = caminho.with_name(f"{caminho.stem}_{timestamp}.xlsx")
            # Excel tem limite de 1.048.576 linhas
            max_rows = min(len(df), 1048576)
            if len(df) > max_rows:
                print(f"   ⚠️  Excel limitado a {max_rows:,} registros de {len(df):,}")
            df.head(max_rows).to_excel(arquivo, index=False, engine='openpyxl')
        elif formato == 'TXT':
            arquivo = caminho.with_name(f"{caminho.stem}_{timestamp}.txt")
            # TXT com UTF-8
            df.to_csv(arquivo, index=False, sep='\t', encoding='utf-8', lineterminator='\n')
            print(f"   💾 Arquivo TXT salvo com encoding UTF-8")
        else:
            return False, f"Formato inválido: {formato}"
        
        size_mb = arquivo.stat().st_size / (1024 * 1024)
        rows_exported = min(len(df), max_rows if formato == 'Excel' else len(df))
        return True, f"✅ Salvo: {arquivo.name} ({size_mb:.2f} MB, {rows_exported:,} registros)"
        
    except Exception as e:
        print(f"❌ Erro na exportação: {e}")
        traceback.print_exc()
        return False, f"❌ Erro: {str(e)}"


def gerar_relatorio(df):
    """Gera relatório dos dados"""
    try:
        relatorio = []
        relatorio.append("=" * 60)
        relatorio.append("📊 RELATÓRIO SISTEMA SIA")
        relatorio.append("=" * 60)
        relatorio.append(f"📅 Gerado: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        relatorio.append(f"📈 Total registros: {len(df):,}")
        relatorio.append(f"📋 Total colunas: {len(df.columns)}")

        # Colunas importantes
        relatorio.append("\n🔍 COLUNAS IMPORTANTES:")
        importantes = ['ID_PACIENTE', 'CNS_PADRONIZADO', 'AP_CNSPCN', 'AP_MUNPCN', 
                      'AP_CIDPRI', 'AP_PRIPAL', 'AP_VL_AP', 'UF', 'ANO', 'MES']
        
        for col in importantes:
            if col in df.columns:
                if col == 'ID_PACIENTE':
                    unicos = df[col].nunique()
                    relatorio.append(f"  • {col}: {unicos:,} pacientes únicos")
                elif col == 'CNS_PADRONIZADO':
                    sem_cns = (df[col] == 'SEM_CNS').sum()
                    com_cns = len(df) - sem_cns
                    relatorio.append(f"  • {col}: {com_cns:,} válidos, {sem_cns:,} sem CNS")
                else:
                    nao_nulos = df[col].notna().sum()
                    relatorio.append(f"  • {col}: {nao_nulos:,} não nulos")
        
        return "\n".join(relatorio)
        
    except Exception as e:
        return f"❌ Erro no relatório: {str(e)}"


class JanelaCarregamento:
    """Janela de carregamento"""

    def __init__(self, parent, mensagem="Processando..."):
        self.janela = tk.Toplevel(parent)
        self.janela.title("Aguarde")
        self.janela.geometry("300x100")
        self.janela.transient(parent)
        self.janela.grab_set()
        
        # Centralizar
        parent.update_idletasks()
        x = parent.winfo_x() + (parent.winfo_width() // 2) - 150
        y = parent.winfo_y() + (parent.winfo_height() // 2) - 50
        self.janela.geometry(f"+{x}+{y}")
        
        # Conteúdo
        frame = ttk.Frame(self.janela, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)
        
        self.label = ttk.Label(frame, text=mensagem, font=('Arial', 10))
        self.label.pack(pady=5)
        
        self.progress = ttk.Progressbar(frame, mode='indeterminate', length=200)
        self.progress.pack(pady=5)
        self.progress.start(10)

    def atualizar(self, mensagem):
        self.label.config(text=mensagem)
        self.janela.update()

    def fechar(self):
        self.progress.stop()
        self.janela.destroy()


class SIAApp:
    """Aplicação principal"""

    def __init__(self, root):
        self.root = root
        self.root.title("SIA APAC Medicamentos")
        self.root.geometry("1200x800")
        
        self.df_original = None
        self.df_processado = None
        self.downloader = SIADownloader()
        
        self.setup_ui()

    def setup_ui(self):
        """Configura interface"""
        # Frame principal
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Layout
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=3)
        main_frame.rowconfigure(0, weight=1)
        
        # ===== COLUNA ESQUERDA (CONTROLES) =====
        left_frame = ttk.Frame(main_frame)
        left_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        
        # Título
        title = ttk.Label(left_frame, text="SIA APAC Medicamentos", 
                         font=('Arial', 14, 'bold'), foreground='darkblue')
        title.pack(pady=(0, 20))
        
        # SEÇÃO 1: DOWNLOAD
        section1 = ttk.LabelFrame(left_frame, text="1. DOWNLOAD DADOS", padding="10")
        section1.pack(fill=tk.X, pady=(0, 15))
        
        # UF
        frame_uf = ttk.Frame(section1)
        frame_uf.pack(fill=tk.X, pady=5)
        ttk.Label(frame_uf, text="UF:").pack(side=tk.LEFT, padx=(0, 5))
        self.uf_var = tk.StringVar(value="MG")
        uf_combo = ttk.Combobox(frame_uf, textvariable=self.uf_var, 
                               values=list(ESTADOS.keys()), width=8, state='readonly')
        uf_combo.pack(side=tk.LEFT)
        
        # Ano
        frame_ano = ttk.Frame(section1)
        frame_ano.pack(fill=tk.X, pady=5)
        ttk.Label(frame_ano, text="Ano:").pack(side=tk.LEFT, padx=(0, 5))
        self.ano_var = tk.StringVar(value="2024")
        ano_combo = ttk.Combobox(frame_ano, textvariable=self.ano_var, 
                                values=["2024", "2023", "2022"], width=8, state='readonly')
        ano_combo.pack(side=tk.LEFT)
        
        # Botão download
        self.btn_download = ttk.Button(section1, text="🌐 Baixar DBC", 
                                      command=self.download_and_process,
                                      width=20)
        self.btn_download.pack(pady=10)
        
        # Status
        self.status_download = ttk.Label(section1, text="Pronto", foreground="green")
        self.status_download.pack()
        
        # SEÇÃO 2: ARQUIVO LOCAL
        section2 = ttk.LabelFrame(left_frame, text="2. CARREGAR ARQUIVO", padding="10")
        section2.pack(fill=tk.X, pady=(0, 15))
        
        frame_file = ttk.Frame(section2)
        frame_file.pack(fill=tk.X, pady=5)
        
        self.file_path = tk.StringVar()
        ttk.Entry(frame_file, textvariable=self.file_path).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(frame_file, text="📁", width=3, 
                  command=self.browse_file).pack(side=tk.LEFT)
        
        ttk.Button(section2, text="📥 Carregar", 
                  command=self.load_file, width=20).pack(pady=5)
        
        # SEÇÃO 3: PROCESSAMENTO
        section3 = ttk.LabelFrame(left_frame, text="3. PROCESSAMENTO", padding="10")
        section3.pack(fill=tk.X, pady=(0, 15))
        
        ttk.Button(section3, text="🔍 Processar CNS", 
                  command=self.process_cns, width=20).pack(pady=5)
        
        ttk.Button(section3, text="📊 Gerar Relatório", 
                  command=self.generate_report, width=20).pack(pady=5)
        
        # SEÇÃO 4: EXPORTAÇÃO
        section4 = ttk.LabelFrame(left_frame, text="4. EXPORTAÇÃO", padding="10")
        section4.pack(fill=tk.X, pady=(0, 15))
        
        self.export_format = tk.StringVar(value="Parquet")
        
        frame_format = ttk.Frame(section4)
        frame_format.pack(fill=tk.X, pady=5)
        
        ttk.Radiobutton(frame_format, text="CSV", 
                       variable=self.export_format, value="CSV").pack(side=tk.LEFT, padx=(0, 10))
        ttk.Radiobutton(frame_format, text="Parquet", 
                       variable=self.export_format, value="Parquet").pack(side=tk.LEFT, padx=(0, 10))
        ttk.Radiobutton(frame_format, text="Excel", 
                       variable=self.export_format, value="Excel").pack(side=tk.LEFT)
        
        ttk.Button(section4, text="💾 Exportar", 
                  command=self.export_data, width=20).pack(pady=10)
        
        # SEÇÃO 5: LOG
        section5 = ttk.LabelFrame(left_frame, text="📋 LOG", padding="10")
        section5.pack(fill=tk.BOTH, expand=True)
        
        text_frame = ttk.Frame(section5)
        text_frame.pack(fill=tk.BOTH, expand=True)
        
        self.text_log = tk.Text(text_frame, width=50, height=20, wrap=tk.WORD,
                               font=('Consolas', 9), bg='#f8f9fa')
        scrollbar = ttk.Scrollbar(text_frame, command=self.text_log.yview)
        self.text_log.configure(yscrollcommand=scrollbar.set)
        
        self.text_log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Mensagem inicial
        self.log("=== SISTEMA SIA ===")
        self.log("=" * 40)
        self.log("\n🚀 INSTRUÇÕES:")
        self.log("1. Selecione UF e Ano")
        self.log("2. Clique em 'Baixar DBC'")
        self.log("3. Sistema converterá automaticamente")
        self.log("4. Clique em 'Processar CNS'")
        self.log("5. Exporte os dados")
        
        # ===== COLUNA DIREITA (VISUALIZAÇÃO) =====
        right_frame = ttk.Frame(main_frame)
        right_frame.grid(row=0, column=1, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Título visualização
        ttk.Label(right_frame, text="👁️ VISUALIZAÇÃO", 
                 font=('Arial', 14, 'bold'), foreground='darkgreen').pack(pady=(0, 10))
        
        # Informações
        self.info_label = ttk.Label(right_frame, text="⏳ Nenhum dado", 
                                   font=('Arial', 10), foreground='gray')
        self.info_label.pack(pady=(0, 10))
        
        # Treeview com scrollbars
        tree_frame = ttk.Frame(right_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        # Criar canvas para scroll horizontal
        canvas = tk.Canvas(tree_frame, borderwidth=0, highlightthickness=0)
        
        # Scrollbars
        v_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        h_scroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL)
        
        # Tree
        self.tree = ttk.Treeview(tree_frame, 
                                yscrollcommand=v_scroll.set,
                                xscrollcommand=h_scroll.set,
                                show='tree headings',
                                selectmode='extended',
                                height=25)
        
        v_scroll.config(command=self.tree.yview)
        h_scroll.config(command=self.tree.xview)
        
        # Layout tree com grid para melhor controle
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        v_scroll.grid(row=0, column=1, sticky=(tk.N, tk.S))
        h_scroll.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        # Configurar pesos para expansão correta
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        
        # Configurar pesos globais
        left_frame.columnconfigure(0, weight=1)
        left_frame.rowconfigure(5, weight=1)  # LOG expande
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(2, weight=1)  # Tree expande

    def log(self, mensagem):
        """Adiciona ao log"""
        self.text_log.insert(tk.END, mensagem + "\n")
        self.text_log.see(tk.END)
        self.root.update()

    def browse_file(self):
        """Seleciona arquivo"""
        filename = filedialog.askopenfilename(
            title="Selecionar arquivo",
            filetypes=[
                ("DBC", "*.dbc"),
                ("CSV", "*.csv"),
                ("Parquet", "*.parquet"),
                ("Excel", "*.xlsx"),
                ("Todos", "*.*")
            ]
        )
        if filename:
            self.file_path.set(filename)
            self.log(f"📁 Selecionado: {Path(filename).name}")

    def download_and_process(self):
        """Download e processamento"""
        uf = self.uf_var.get()
        ano = self.ano_var.get()
        
        if not uf or not ano:
            messagebox.showerror("Erro", "Selecione UF e Ano")
            return
        
        self.log(f"\n{'='*60}")
        self.log(f"🚀 DOWNLOAD {uf}-{ano}")
        self.log(f"{'='*60}")
        
        # Desabilitar botão
        self.btn_download.config(state='disabled')
        self.status_download.config(text="Baixando...", foreground="blue")
        
        # Janela carregamento
        janela = JanelaCarregamento(self.root, "Conectando ao DATASUS...")
        
        def processar():
            try:
                janela.atualizar("Baixando dados...")
                
                # Download e conversão
                df, total = self.downloader.processar_estado(uf, int(ano))
                
                if df is not None and total > 0:
                    self.df_original = df
                    
                    janela.atualizar("Processando CNS...")
                    
                    # Processar CNS
                    df_proc, erro = processar_cns(df)
                    
                    if df_proc is not None:
                        self.df_processado = df_proc
                        
                        janela.fechar()
                        
                        self.log(f"\n✅ CONCLUÍDO!")
                        self.log(f"📊 Registros: {total:,}")
                        self.log(f"📋 Colunas: {len(df.columns)}")
                        
                        if 'ID_PACIENTE' in df_proc.columns:
                            unicos = df_proc['ID_PACIENTE'].nunique()
                            self.log(f"👥 Pacientes únicos: {unicos:,}")
                        
                        # Mostrar dados
                        self.root.after(0, self.mostrar_dados)
                        self.status_download.config(text=f"✅ {total:,} registros", foreground="green")
                    else:
                        janela.fechar()
                        self.log(f"\n⚠️  DADOS BAIXADOS")
                        self.log(f"📊 Registros: {total:,}")
                        self.log(f"💡 {erro}")
                        
                        self.root.after(0, self.mostrar_dados)
                        self.status_download.config(text=f"⚠️  {total:,} registros", foreground="orange")
                else:
                    janela.fechar()
                    self.log(f"\n❌ FALHA NO DOWNLOAD")
                    self.status_download.config(text="❌ Falha", foreground="red")
                    
            except Exception as e:
                janela.fechar()
                self.log(f"\n❌ ERRO: {str(e)}")
                traceback.print_exc()
                self.status_download.config(text="❌ Erro", foreground="red")
            finally:
                self.root.after(0, lambda: self.btn_download.config(state='normal'))
        
        # Thread
        threading.Thread(target=processar, daemon=True).start()

    def load_file(self):
        """Carrega arquivo local"""
        if not self.file_path.get():
            messagebox.showerror("Erro", "Selecione arquivo")
            return
        
        self.log(f"\n📂 CARREGANDO ARQUIVO...")
        
        janela = JanelaCarregamento(self.root, "Carregando...")
        
        def carregar():
            try:
                df, erro = carregar_arquivo(self.file_path.get())
                
                janela.fechar()
                
                if erro:
                    messagebox.showerror("Erro", erro)
                    self.log(f"❌ {erro}")
                else:
                    self.df_original = df
                    self.log(f"\n✅ CARREGADO!")
                    self.log(f"📊 Registros: {len(df):,}")
                    self.log(f"📋 Colunas: {len(df.columns)}")
                    
                    self.root.after(0, self.mostrar_dados)
                    
            except Exception as e:
                janela.fechar()
                self.log(f"❌ ERRO: {str(e)}")
        
        threading.Thread(target=carregar, daemon=True).start()

    def mostrar_dados(self):
        """Mostra dados na treeview"""
        if self.df_original is None:
            return

        # Limpar tree
        for item in self.tree.get_children():
            self.tree.delete(item)

        # Qual DataFrame mostrar
        df_show = self.df_processado if self.df_processado is not None else self.df_original

        # 🔍 DEBUG: Mostrar amostra do AP_CNSPCN no terminal
        print("\n" + "="*60)
        print("🔍 DEBUG - AMOSTRA AP_CNSPCN NA VISUALIZAÇÃO:")
        print("="*60)
        if 'AP_CNSPCN' in df_show.columns:
            amostra = df_show['AP_CNSPCN'].head(10).tolist()
            for i, valor in enumerate(amostra, 1):
                # Mostrar tipo, repr e valor
                print(f"{i:2d}. Tipo: {type(valor).__name__:10s} | repr(): {repr(valor)[:50]:50s} | str(): {str(valor)[:30]}")

            # Mostrar também em bytes se for string
            primeiro = df_show['AP_CNSPCN'].iloc[0]
            if isinstance(primeiro, str):
                print(f"\n📊 Primeiro valor em bytes: {primeiro.encode('utf-8', errors='replace')}")
        else:
            print("❌ Coluna AP_CNSPCN não encontrada!")

        if 'CNS_PADRONIZADO' in df_show.columns:
            print(f"\n✅ CNS_PADRONIZADO amostra: {df_show['CNS_PADRONIZADO'].head(5).tolist()}")

        print("="*60 + "\n")
        
        # Colunas para mostrar - TODAS as colunas importantes ou primeiras 15
        colunas_imp = ['ID_PACIENTE', 'CNS_PADRONIZADO', 'AP_CNSPCN', 'AP_MUNPCN',
                      'AP_CIDPRI', 'AP_PRIPAL', 'AP_VL_AP', 'AP_UFMUN', 'UF', 'ANO', 'MES']
        
        # Tentar colunas importantes primeiro
        colunas = [c for c in colunas_imp if c in df_show.columns]
        
        # Se não tiver colunas importantes, pegar todas (até 15)
        if not colunas:
            colunas = list(df_show.columns)[:15]
        else:
            # Adicionar outras colunas até completar 15
            outras = [c for c in df_show.columns if c not in colunas]
            colunas.extend(outras[:max(0, 15 - len(colunas))])
        
        # Configurar tree
        self.tree["columns"] = colunas
        self.tree["displaycolumns"] = colunas
        
        # Configurar coluna #0 (índice)
        self.tree.heading("#0", text="#", anchor='center')
        self.tree.column("#0", width=60, minwidth=60, stretch=False, anchor='center')
        
        # Configurar cada coluna
        for col in colunas:
            # Nome exibido (truncar se muito longo)
            nome_exibido = col if len(col) <= 20 else col[:17] + "..."
            
            self.tree.heading(col, text=nome_exibido, anchor='w')
            
            # Calcular largura baseada no conteúdo
            # Largura mínima: 100px
            # Largura baseada no nome da coluna
            col_width = max(100, len(col) * 8 + 20)
            
            # Verificar tamanho máximo dos dados (primeiras 100 linhas)
            if len(df_show) > 0:
                sample = df_show[col].head(100).astype(str)
                max_len = sample.str.len().max()
                data_width = min(max_len * 8 + 20, 400)  # Máximo 400px
                col_width = max(col_width, data_width)
            
            self.tree.column(col, width=col_width, minwidth=80, stretch=True, anchor='w')
        
        # Adicionar dados (primeiras 1000 linhas para performance)
        df_preview = df_show.head(1000)
        
        for idx, row in df_preview.iterrows():
            valores = []
            for col in colunas:
                val = row[col]
                if pd.isna(val):
                    valores.append("")
                else:
                    val_str = str(val)
                    # Truncar valores muito longos
                    if len(val_str) > 50:
                        valores.append(val_str[:47] + "...")
                    else:
                        valores.append(val_str)
            
            self.tree.insert("", tk.END, text=str(idx+1), values=valores)
        
        # Atualizar informações
        total = len(df_show)
        cols = len(df_show.columns)
        showing = min(1000, total)
        
        info = f"📊 {total:,} registros ({showing:,} exibidos) | 📋 {cols} colunas"
        
        if self.df_processado is not None and 'ID_PACIENTE' in self.df_processado.columns:
            unicos = self.df_processado['ID_PACIENTE'].nunique()
            info += f" | 👥 {unicos:,} pacientes únicos"
        
        self.info_label.config(text=info)
        
        print(f"\n📺 Visualização atualizada:")
        print(f"   • Colunas exibidas: {len(colunas)}")
        print(f"   • Registros exibidos: {showing:,} de {total:,}")

    def process_cns(self):
        """Processa CNS"""
        if self.df_original is None:
            messagebox.showerror("Erro", "Carregue dados primeiro")
            return
        
        self.log(f"\n🔍 PROCESSANDO CNS...")
        
        janela = JanelaCarregamento(self.root, "Processando...")
        
        def processar():
            try:
                df_proc, erro = processar_cns(self.df_original)
                
                janela.fechar()
                
                if erro:
                    messagebox.showerror("Erro", erro)
                    self.log(f"❌ {erro}")
                else:
                    self.df_processado = df_proc
                    self.log(f"\n✅ CNS PROCESSADO!")
                    
                    self.root.after(0, self.mostrar_dados)
                    
            except Exception as e:
                janela.fechar()
                self.log(f"❌ ERRO: {str(e)}")
        
        threading.Thread(target=processar, daemon=True).start()

    def generate_report(self):
        """Gera relatório"""
        df = self.df_processado if self.df_processado is not None else self.df_original
        if df is None:
            messagebox.showerror("Erro", "Carregue dados primeiro")
            return
        
        self.log(f"\n{'='*60}")
        self.log("📊 RELATÓRIO...")
        
        relatorio = gerar_relatorio(df)
        self.log(relatorio)

    def export_data(self):
        """Exporta dados"""
        df = self.df_processado if self.df_processado is not None else self.df_original
        if df is None:
            messagebox.showerror("Erro", "Carregue dados primeiro")
            return
        
        # Sugerir nome
        nome = "SIA_dados"
        if 'UF' in df.columns and 'ANO' in df.columns:
            uf = df['UF'].iloc[0] if len(df) > 0 else "UF"
            ano = df['ANO'].iloc[0] if len(df) > 0 else "ANO"
            nome = f"SIA_{uf}_{ano}"
        
        # Extensões
        exts = {'CSV': '.csv', 'Parquet': '.parquet', 'Excel': '.xlsx', 'TXT': '.txt'}
        formato = self.export_format.get()
        ext = exts.get(formato, '.csv')
        
        filename = filedialog.asksaveasfilename(
            title=f"Salvar como {formato}",
            defaultextension=ext,
            initialfile=nome + ext,
            filetypes=[(f"{formato}", f"*{ext}")]
        )
        
        if not filename:
            return
        
        self.log(f"\n💾 EXPORTANDO...")
        
        janela = JanelaCarregamento(self.root, f"Exportando {formato}...")
        
        def exportar():
            try:
                sucesso, msg = exportar_dados(df, filename, formato)
                
                janela.fechar()
                
                if sucesso:
                    self.log(f"✅ {msg}")
                    messagebox.showinfo("Sucesso", "Exportado!")
                else:
                    self.log(f"❌ {msg}")
                    messagebox.showerror("Erro", msg)
                    
            except Exception as e:
                janela.fechar()
                self.log(f"❌ ERRO: {str(e)}")
        
        threading.Thread(target=exportar, daemon=True).start()


if __name__ == "__main__":
    print("=" * 60)
    print("SISTEMA SIA APAC MEDICAMENTOS")
    print("=" * 60)
    print("✅ Conversor DBC embutido")
    print("✅ Download automático do DATASUS")
    print("✅ Processamento CNS")
    print("✅ Interface gráfica completa")
    print("=" * 60)

    # Iniciar
    try:
        root = tk.Tk()
        app = SIAApp(root)
        
        # Centralizar
        root.update_idletasks()
        w = root.winfo_width()
        h = root.winfo_height()
        x = (root.winfo_screenwidth() // 2) - (w // 2)
        y = (root.winfo_screenheight() // 2) - (h // 2)
        root.geometry(f'{w}x{h}+{x}+{y}')
        
        root.mainloop()
        
    except Exception as e:
        print(f"❌ ERRO: {e}")
        traceback.print_exc()
        input("Enter para sair...")

    print("🎯 Sistema finalizado com sucesso!")