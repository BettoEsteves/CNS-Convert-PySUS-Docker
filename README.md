# SIA APAC Medicamentos - Sistema de Processamento CNS

Sistema completo para download, processamento e validação de dados do SIA/DATASUS com foco em APAC Medicamentos, incluindo validação de CNS (Cartão Nacional de Saúde) conforme especificação e-SUS.

# Versão 1.0

## ✨ Características

- ⬇️ Download automático de arquivos DBC do DATASUS via FTP
- 🐳 Conversão de arquivos DBC usando Docker + PySUS
- 🔢 Processamento correto do campo AP_CNSPCN (15 dígitos com zeros à esquerda)
- ✅ Validação de CNS conforme algoritmo oficial e-SUS
- 🖥️ Interface gráfica completa com Tkinter
- 📊 Exportação para CSV, Parquet, Excel e TXT
- 👁️ Visualização de dados em tempo real
- 📈 Estatísticas detalhadas de validação

## 📋 Pré-requisitos

### Windows

1. **Python 3.9 ou superior**
   - Download: https://www.python.org/downloads/
   - Durante instalação, marque "Add Python to PATH"

2. **Docker Desktop**
   - Download: https://www.docker.com/products/docker-desktop
   - Necessário para conversão de arquivos DBC
   - Após instalar, inicie o Docker Desktop e aguarde inicializar

3. **Git** (opcional, para clonar repositório)
   - Download: https://git-scm.com/downloads

## 🚀 Instalação

### Passo 1: Clonar ou Baixar o Projeto

```bash
# Opção 1: Clonar com Git
git clone https://github.com/BettoEsteves/CNS-Convert-PySUS-Docker.git
cd CNS-Convert-PySUS-Docker

# Opção 2: Baixar ZIP e extrair
# Clique em "Code" > "Download ZIP" no GitHub
```

### Passo 2: Instalar Docker Desktop

1. Baixe e instale o Docker Desktop: https://www.docker.com/products/docker-desktop
2. Abra o Docker Desktop
3. Aguarde até o ícone ficar verde (Docker rodando)

### Passo 3: Executar Instalador Automático

```batch
# Execute o instalador (clique duplo ou via terminal)
install.bat
```

O instalador irá:
1. ✅ Verificar se Dockerfile existe
2. ✅ Verificar instalação do Python
3. ✅ Verificar se Docker está rodando
4. ✅ Construir imagem Docker `pysus:local` (aguarde 2-5 minutos)

### Passo 4: Criar Ambiente Virtual Python

```batch
# Criar ambiente virtual
python -m venv .venv_sia

# Ativar ambiente virtual (Windows)
.venv_sia\Scripts\activate
```

### Passo 5: Instalar Dependências Python

```batch
# Com ambiente virtual ativado
pip install -r requirements.txt
```

## 📁 Estrutura do Projeto

```
CNS-Convert-PySUS-Docker/
├── SIA_Conv_CNS.py       # Programa principal
├── requirements.txt       # Dependências Python
├── Dockerfile            # Imagem Docker PySUS
├── install.bat          # Instalador automático
├── README.md           # Este arquivo
├── .gitignore          # Arquivos ignorados pelo Git
├── dados/             # Diretório para dados processados (criado automaticamente)
└── .venv_sia/        # Ambiente virtual Python (após instalação)
```

## 🎮 Uso

### Executar o Sistema

```batch
# 1. Certifique-se que Docker Desktop está rodando

# 2. Ative o ambiente virtual
.venv_sia\Scripts\activate

# 3. Execute o sistema
python SIA_Conv_CNS.py
```

### Interface Gráfica

A interface possui 5 seções principais:

#### 1. DOWNLOAD DADOS
- Selecione **UF** (estado) e **Ano**
- Clique em **"Baixar DBC"**
- O sistema irá:
  - Baixar arquivo DBC do DATASUS via FTP
  - Converter usando Docker + PySUS
  - Processar CNS automaticamente
  - Exibir dados na tela

#### 2. CARREGAR ARQUIVO LOCAL
- Clique no botão **📂** para selecionar arquivo
- Formatos suportados:
  - `.dbc` (será convertido via Docker)
  - `.csv` (várias codificações suportadas)
  - `.parquet` (formato otimizado)
  - `.xlsx` (Excel)
- Clique em **"Carregar"**

#### 3. PROCESSAMENTO
- **Processar CNS**: Limpa, padroniza e valida CNS
  - Cria campo `CNS_PADRONIZADO` (15 dígitos com zeros à esquerda)
  - Cria campo `CNS_VALIDO` (True/False)
  - Cria campo `CNS_TIPO` (definitivo/provisorio/invalido/sem_cns)
  - Cria campo `ID_PACIENTE` (município + CNS - chave única)

- **Gerar Relatório**: Estatísticas completas dos dados

#### 4. EXPORTAÇÃO
- Escolha formato: **CSV**, **Parquet**, **Excel** ou **TXT**
- Clique em **"Exportar"**
- Arquivo salvo com timestamp no nome

#### 5. LOG
- Acompanhe todas as operações em tempo real
- Mensagens de erro e sucesso
- Estatísticas de processamento
- Debug de validação CNS

## 🔐 Validação CNS

O sistema implementa o algoritmo oficial de validação CNS conforme [especificação e-SUS Bridge UFSC](https://integracao.esusaps.bridge.ufsc.tech/v211/docs/algoritmo_CNS.html):

### CNS Definitivo (inicia com 1 ou 2)
- Valida dígito verificador usando PIS (primeiros 11 dígitos)
- Formato: 15 dígitos numéricos
- Exemplos: `123456789012345`, `234567890123456`

### CNS Provisório (inicia com 7, 8 ou 9)
- Valida soma ponderada de todos os 15 dígitos
- Formato: 15 dígitos numéricos
- Exemplos: `789012345678901`, `890123456789012`

### Campos Gerados
- `CNS_PADRONIZADO`: CNS com 15 dígitos e zeros à esquerda preservados
- `CNS_VALIDO`: Boolean (True = CNS válido, False = inválido)
- `CNS_TIPO`: String com tipo do CNS:
  - `definitivo` - CNS válido iniciando com 1 ou 2
  - `provisorio` - CNS válido iniciando com 7, 8 ou 9
  - `invalido` - CNS que não passou na validação
  - `sem_cns` - Registro sem CNS

## 🔄 Conversão de Arquivos DBC

Os arquivos DBC do DATASUS usam:
- **Compressão**: PKWare/DBF comprimido
- **Encoding**: Latin-1 (ISO-8859-1)
- **Formato especial**: Dígitos codificados como caracteres Latin-1

### Mapeamento de Caracteres para Dígitos

O sistema converte automaticamente caracteres especiais Latin-1 para dígitos:

| Hex  | Caractere | Dígito |
|------|-----------|--------|
| 0x7B | {         | 0      |
| 0x7C | pipe      | 1      |
| 0x7D | }         | 2      |
| 0x7E | ~         | 3      |
| 0x7F | DEL       | 4      |
| 0x80 | euro      | 5      |
| 0x81 | control   | 6      |
| 0x82 | comma     | 7      |
| 0x83 | f-hook    | 8      |
| 0x84 | quote     | 9      |

**Exemplo de conversão:**
- **Arquivo DBC bruto**: `\x82{\x83\x81{|{\x82}\x80\x84\x84\x7f\x83\x81` (caracteres especiais)
- **Após conversão**: `701831071099486` (15 dígitos numéricos válidos)

## 🔧 Solução de Problemas

### Erro: "Docker não encontrado"
**Causa:** Docker Desktop não está instalado ou não está rodando

**Solução:**
1. Instale Docker Desktop: https://www.docker.com/products/docker-desktop
2. Abra Docker Desktop
3. Aguarde o ícone ficar verde (Docker inicializado)
4. Execute `install.bat` novamente

### Erro: "Imagem pysus:local não encontrada"
**Causa:** Imagem Docker não foi construída

**Solução:**
```batch
# Reconstruir imagem manualmente
docker build -t pysus:local .

# Verificar se foi criada
docker images pysus:local
```

### Erro: "CNS todos inválidos" ou "CNS em ASCII"
**Causa:** Encoding incorreto durante conversão DBC

**Solução:**
- ✅ O sistema já corrige automaticamente (versão atual)
- Certifique-se de estar usando `SIA_Conv_CNS.py` atualizado
- Verifique que a imagem Docker está atualizada

### Erro: "ModuleNotFoundError"
**Causa:** Dependências Python não instaladas

**Solução:**
```batch
# Ativar ambiente virtual
.venv_sia\Scripts\activate

# Reinstalar dependências
pip install -r requirements.txt
```

### Erro: "Timeout na conversão Docker"
**Causa:** Arquivo DBC muito grande ou Docker lento

**Solução:**
- Verifique espaço em disco disponível
- Feche outros aplicativos pesados
- Aguarde mais tempo (arquivos grandes podem levar até 5 minutos)

## 🔄 Atualização

Para atualizar o sistema:

```batch
# 1. Baixar nova versão
git pull

# 2. Reconstruir imagem Docker (se Dockerfile mudou)
docker build -t pysus:local .

# 3. Atualizar dependências Python
pip install -r requirements.txt --upgrade
```

## 📝 Notas Técnicas

### Encoding
- **Arquivos DBC**: Latin-1 (ISO-8859-1)
- **Processamento interno**: UTF-8
- **Exportação CSV**: UTF-8 com BOM (compatível com Excel)
- **Exportação TXT**: UTF-8

### Performance
- **Leitura DBC**: ~30 segundos para 100k registros
- **Processamento CNS**: ~5 segundos para 100k registros
- **Validação CNS**: ~10 segundos para 100k registros

### Limitações
- **Excel**: Máximo 1.048.576 linhas (limite do formato)
- **Visualização**: Mostra primeiras 1.000 linhas (por performance)
- **Docker timeout**: 5 minutos para conversão (ajustável)

### Tecnologias Utilizadas
- **Python**: 3.9+
- **Docker**: Container para PySUS
- **PySUS**: Biblioteca oficial DATASUS ([AlertaDengue/PySUS](https://github.com/AlertaDengue/PySUS))
- **pyreaddbc**: Leitura de arquivos DBC
- **pandas**: Manipulação de dados
- **tkinter**: Interface gráfica

### Telas
<img width="1917" height="1008" alt="image" src="https://github.com/user-attachments/assets/8d466ddf-90f6-48ac-8442-ffafbb84235a" />


<img width="1913" height="1021" alt="image" src="https://github.com/user-attachments/assets/b67d62aa-7913-4c8c-b008-ce732b67b8fd" />


## 👥 Autores

- **Carolina Jacoby** - Pesquisadora / Desenvolvedor
- **Betto Esteves** - Desenvolvedor ([GitHub](https://github.com/BettoEsteves))
- **Claude Code (Sonnet 4.5)** - Assistente de Desenvolvimento IA

## 🙏 Créditos

Este projeto utiliza e agradece às seguintes bibliotecas de código aberto:

- **PySUS** - Equipe AlertaDengue: https://github.com/AlertaDengue/PySUS
```batch
@software{flavio_codeco_coelho_2021_4883502,
  author       = {Flávio Codeço Coelho and
                  Bernardo Chrispim Baron and
                  Gabriel Machado de Castro Fonseca and
                  Pedro Reck and
                  Daniela Palumbo},
  title        = {AlertaDengue/PySUS: Vaccine},
  month        = may,
  year         = 2021,
  publisher    = {Zenodo},
  version      = {0.5.17},
  doi          = {10.5281/zenodo.4883502},
  url          = {https://doi.org/10.5281/zenodo.4883502}
}
```
  - Biblioteca Python para acesso aos dados do DATASUS
  - Essencial para download e processamento de dados do SIA/SUS

- **pyreaddbc** - Comunidade Python
  - Conversão de arquivos DBC (DBF comprimido)

- **pandas** - Equipe de desenvolvimento Pandas
  - Manipulação e análise de dados

## 🤝 Contribuindo

Contribuições são bem-vindas! Para contribuir:

1. Fork o repositório
2. Crie uma branch para sua feature (`git checkout -b feature/MinhaFeature`)
3. Commit suas mudanças (`git commit -m 'Adiciona MinhaFeature'`)
4. Push para a branch (`git push origin feature/MinhaFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto é fornecido "como está", sem garantias de qualquer tipo.

## 📚 Referências

- [DATASUS - SIA/SUS](http://sia.datasus.gov.br/)
- [PySUS - Python DATASUS](https://github.com/AlertaDengue/PySUS)
- [pyreaddbc](https://github.com/danicat/pyreaddbc)
- [Algoritmo CNS - e-SUS](https://integracao.esusaps.bridge.ufsc.tech/v211/docs/algoritmo_CNS.html)
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Python](https://www.python.org/)

## 💬 Suporte

Para reportar bugs ou solicitar features, abra uma [issue](https://github.com/BettoEsteves/CNS-Convert-PySUS-Docker/issues) no repositório.

---

**🤖 Desenvolvido com Claude Code (Sonnet 4.5) - Anthropic**

*Este sistema foi desenvolvido para auxiliar pesquisadores e profissionais de saúde no processamento de dados do SIA/DATASUS com foco especial na validação e padronização do CNS (Cartão Nacional de Saúde).*
