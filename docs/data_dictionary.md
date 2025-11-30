📘 Dicionário de Dados – Projeto FIPE
🥉 1. Camada BRONZE (bronze.fipe_raw)

Dados brutos coletados da API FIPE, sem agregações, apenas estruturados em tabela.

Campo	Tipo	Descrição
marca	string	Nome da marca da moto
modelo	string	Nome do modelo da moto
ano_modelo	string	Ano/versão do modelo
codigo_marca	int	Código da marca na FIPE
codigo_modelo	int	Código do modelo na FIPE
codigo_ano	int	Código do ano/versão na FIPE
valor	string	Valor formatado como texto (ex: "R$ 25.000,00")
valor_numeric	float	Valor convertido para número

🥈 2. Camada SILVER (silver.fipe_limited)

Filtragem aplicada aos dados do bronze: apenas motos com valor entre 18k e 30k.

Campo	Tipo	Descrição
marca	string	Nome da marca da moto
modelo	string	Nome do modelo da moto
ano_modelo	string	Ano/versão do modelo
valor_numeric	float	Valor convertido para número

🥇 3. Camada GOLD (gold.fipe_summary)

Agregações realizadas a partir da camada silver: médias por modelo e quantidade de registros.

Campo	Tipo	Descrição
marca	string	Nome da marca da moto
modelo	string	Nome do modelo da moto
media_valor	float	Média do valor das motos do modelo
qtd_registros	int	Número de registros do modelo