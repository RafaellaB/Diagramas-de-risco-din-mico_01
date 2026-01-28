import os

caminho_arquivo = 'tide/mare_calculada_hora_em_hora_ano-completo.csv'

if os.path.exists(caminho_arquivo):
    with open(caminho_arquivo, 'r', encoding='utf-8') as f:
        linhas = f.readlines()

    # Filtra apenas as linhas que NÃO são marcas de conflito
    linhas_limpas = [l for l in linhas if not l.startswith(('<<<<', '====', '>>>>'))]

    with open(caminho_arquivo, 'w', encoding='utf-8') as f:
        f.writelines(linhas_limpas)
    
    print("✅ Arquivo de maré limpo com sucesso! Marcas de conflito removidas.")
else:
    print("❌ Arquivo não encontrado. Verifique se o caminho 'tide/mare...' está correto.")