main — versão estável

dev — integrações

feature/etl-raw — sua branch para ingestão

feature/etl-transform — sua branch para transformações
Commits pequenos e descritivos: EX: feat(etl): add raw ingestion script,
fix(transform): handle missing total_amount.


### Checklist de entrega (o que você deve subir pro GitHub)

 /src/etl com scripts: extract.py, transform.py, load.py, run_pipeline.py.

 /src/sql com scripts de criação de schemas/tabelas.

 /infra/docker-compose.yml (postgres).

 /datasets/sample_data.csv com dados de teste.

 /src/tests com pelo menos 1 teste.

 Atualizar README.md com instruções de execução.

 Criar PR para main com descrição das mudanças e responsabilidades