# Para implementar a solução senquencial para processar os arquivos .csv o primeiro passo para ler o arquivo .csv e armazenar os dados
# Segundo passso e Implementar as Operações - Sendo eles o cálculo de Valor Total Pago por Estado (UF), Contagem de Beneficiários por Município, Média do Valor da Parcela por Estado e a Média do Valor da Parcela por Estado
# O logging ele adicionara um sistema para rastrear o progresso e eventuais problemas durante a execução.

import csv
import logging
from collections import defaultdict

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CSVProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.total_pago_por_estado = defaultdict(float)
        self.contagem_beneficiarios_por_municipio = defaultdict(int)
        self.soma_parcelas_por_estado = defaultdict(float)
        self.contagem_parcelas_por_estado = defaultdict(int)
        self.media_valor_parcela_por_estado = {}

    def validar_dados(self, row):
        try:
            uf = row['UF']
            municipio = row['Municipio']
            valor_parcela = float(row['Valor_Parcela'])
            return uf, municipio, valor_parcela
        except ValueError as e:
            logging.error(f"Erro ao converter dados: {e} - Linha: {row}")
            return None, None, None
        except KeyError as e:
            logging.error(f"Chave não encontrada: {e} - Linha: {row}")
            return None, None, None

    def processar_linha(self, row):
        uf, municipio, valor_parcela = self.validar_dados(row)
        if uf and municipio:
            # Cálculo do Valor Total Pago por Estado (UF)
            self.total_pago_por_estado[uf] += valor_parcela

            # Contagem de Beneficiários por Município
            self.contagem_beneficiarios_por_municipio[municipio] += 1

            # Soma e contagem para calcular a Média do Valor da Parcela por Estado
            self.soma_parcelas_por_estado[uf] += valor_parcela
            self.contagem_parcelas_por_estado[uf] += 1

    def calcular_media_parcelas_por_estado(self):
        for uf in self.soma_parcelas_por_estado:
            self.media_valor_parcela_por_estado[uf] = (
                self.soma_parcelas_por_estado[uf] / self.contagem_parcelas_por_estado[uf]
            )

    def processar_arquivo(self):
        logging.info(f"Iniciando processamento do arquivo: {self.file_path}")
        try:
            with open(self.file_path, mode='r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    self.processar_linha(row)

            # Após processar todas as linhas, calcular a média
            self.calcular_media_parcelas_por_estado()

            logging.info("Processamento concluído com sucesso.")
        except FileNotFoundError as e:
            logging.error(f"Arquivo não encontrado: {e}")
        except Exception as e:
            logging.error(f"Ocorreu um erro durante o processamento: {e}")

    def obter_resultados(self):
        return {
            'total_pago_por_estado': dict(self.total_pago_por_estado),
            'contagem_beneficiarios_por_municipio': dict(self.contagem_beneficiarios_por_municipio),
            'media_valor_parcela_por_estado': self.media_valor_parcela_por_estado
        }

# Um exemplo de uso com um arquivo 
file_path = 'dados.csv'  # Substitua pelo caminho do seu arquivo .csv
processor = CSVProcessor(file_path)
processor.processar_arquivo()
resultados = processor.obter_resultados()

print("Total Pago por Estado:")
print(resultados['total_pago_por_estado'])

print("\nContagem de Beneficiários por Município:")
print(resultados['contagem_beneficiarios_por_municipio'])

print("\nMédia do Valor da Parcela por Estado:")
print(resultados['media_valor_parcela_por_estado'])

# A leitura do CSV e feita utilizando o csv.DictReader para ler o codigo de forma que cada linha seja um "dicionário"
# Usamos "defaultdict" para simplificar a contagem e a soma
# Para cada linha do CSV - O valor total pago por Estado (UF) onde é somado. O númerode beneficiários por município e incrementado e a soma pelo número de parcelas.
# A validação dos dados de processo de cada linha e feita com "validar_dados" onde verifica a presença de chaves esperadas e tenta converter os valores para o tipo adequado.