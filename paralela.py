# Para fazer a divisão de arquivos, divimos os arquivos em partes menores que serão precessadas por diferentes processos, cada processo realiza as operações de cálculo do valor por estado, os resultados individuais de cada processo são agregados para formar o resultado.

import csv
import os
import multiprocessing as mp
from collections import defaultdict

def processar_parte(arquivo_parte):
    total_pago_por_estado = defaultdict(float)
    contagem_beneficiarios_por_municipio = defaultdict(int)
    soma_parcelas_por_estado = defaultdict(float)
    contagem_parcelas_por_estado = defaultdict(int)

    with open(arquivo_parte, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                uf = row['UF']
                municipio = row['Municipio']
                valor_parcela = float(row['Valor_Parcela'])
                
                total_pago_por_estado[uf] += valor_parcela
                contagem_beneficiarios_por_municipio[municipio] += 1
                soma_parcelas_por_estado[uf] += valor_parcela
                contagem_parcelas_por_estado[uf] += 1

            except (ValueError, KeyError) as e:
                print(f"Erro ao processar a linha: {e} - Linha: {row}")

    media_valor_parcela_por_estado = {
        uf: soma_parcelas_por_estado[uf] / contagem_parcelas_por_estado[uf]
        for uf in soma_parcelas_por_estado
    }

    return (
        dict(total_pago_por_estado),
        dict(contagem_beneficiarios_por_municipio),
        media_valor_parcela_por_estado
    )

def combinar_resultados(resultados_parciais):
    total_pago_por_estado = defaultdict(float)
    contagem_beneficiarios_por_municipio = defaultdict(int)
    soma_parcelas_por_estado = defaultdict(float)
    contagem_parcelas_por_estado = defaultdict(int)

    for parcial in resultados_parciais:
        parcial_total_pago, parcial_beneficiarios, parcial_media = parcial
        
        for uf, valor in parcial_total_pago.items():
            total_pago_por_estado[uf] += valor

        for municipio, contagem in parcial_beneficiarios.items():
            contagem_beneficiarios_por_municipio[municipio] += contagem

        for uf, valor in parcial_media.items():
            soma_parcelas_por_estado[uf] += valor * contagem_beneficiarios_por_municipio[uf]
            contagem_parcelas_por_estado[uf] += contagem_beneficiarios_por_municipio[uf]

    media_valor_parcela_por_estado = {
        uf: soma_parcelas_por_estado[uf] / contagem_parcelas_por_estado[uf]
        for uf in soma_parcelas_por_estado
    }

    return (
        dict(total_pago_por_estado),
        dict(contagem_beneficiarios_por_municipio),
        media_valor_parcela_por_estado
    )

def dividir_arquivo(file_path, num_partes):
    file_parts = []
    file_size = os.path.getsize(file_path)
    parte_tamanho = file_size // num_partes
    
    with open(file_path, mode='r') as file:
        headers = file.readline()
        for i in range(num_partes):
            parte_file_name = f'{file_path}_parte{i}'
            with open(parte_file_name, 'w') as parte_file:
                parte_file.write(headers)
                bytes_escritos = 0
                while bytes_escritos < parte_tamanho:
                    linha = file.readline()
                    if not linha:
                        break
                    parte_file.write(linha)
                    bytes_escritos += len(linha)
            file_parts.append(parte_file_name)

    return file_parts

def processar_csv_paralelo(file_path, num_processos):
    file_parts = dividir_arquivo(file_path, num_processos)

    with mp.Pool(processes=num_processos) as pool:
        resultados_parciais = pool.map(processar_parte, file_parts)

    resultados_finais = combinar_resultados(resultados_parciais)

    for parte_file in file_parts:
        os.remove(parte_file)

    return resultados_finais

# Um exemplo de uso com o arquivo
file_path = 'dados.csv'  # Substitua pelo caminho do seu arquivo .csv
num_processos = mp.cpu_count()  # Usa o número de CPUs disponíveis para paralelismo
resultados_finais = processar_csv_paralelo(file_path, num_processos)

print("Total Pago por Estado:")
print(resultados_finais[0])

print("\nContagem de Beneficiários por Município:")
print(resultados_finais[1])

print("\nMédia do Valor da Parcela por Estado:")
print(resultados_finais[2])

# A função "dividir_arquivo" divide o arquivo em várias partes para que cada processo possa lidr com uma parte seprada, onde o arquivo é dividido com base no tamanho do arquivo e no número de processos
# A função "processar_parte" processe uma parte do arquivo.
# A função "combinar_resultados" ele agreg os resultados parciais de todos os processos para formar o resultado final. Isso inclui combinar os valores totais pagos por estado por municipio e a média do valor das parcelas por estado
# Apos o processamento, as partes do arquivo divididas são removidas para liberar espaço em disco.