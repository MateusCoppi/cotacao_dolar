# Projeto de ELT com Docker, Airflow e MinIO

Este projeto utiliza **Docker**, **Apache Airflow** e **MinIO**, todos rodando em containers Docker.

## Pré-requisitos

Antes de começar, certifique-se de ter os seguintes softwares instalados em sua máquina:

- [Docker](https://docs.docker.com/get-docker/) - Para rodar os containers.
- [Docker Compose](https://docs.docker.com/compose/install/) - Para orquestrar os containers.

> **Nota**: O passo a passo para instalação e configuração de cada um desses softwares pode ser encontrado nas respectivas documentações.
> **Obs**: Arquivos padrões de configuração do airflow, docker e criação da Venv que não foram modificados estão no gitignore visto que na instalação das aplicações esses arquivos são instalados de forma padrão.

## Configuração do Ambiente

### 1. Clonar o Repositório

Clone este repositório em sua máquina local:

```bash
git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio
