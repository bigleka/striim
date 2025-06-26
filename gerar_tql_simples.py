# Este arquivo serve como BASE para criar arquivos .TQL que são usado pelo striim como Application para o ETL
# é totalmente possivel fazer a criação dos Applications pela interface web, mas esse arquivo destina-se a ajudar na criação de multiplos arquivos para muitas tabelas.
# é importante ressaltar a necessidade de mudança dos parâmetros para manter a minima organização.
# dependendo da quantidade de tabelas, pode ser gerado uma quantidade considerável de arquivos, tem outro script shell nesse repositório para fazer o upload desses arquivos para o striim.

# este exemplo vai gerar arquivos TQL para a extração de dados do Striim entre um banco SQL Server e o destino é um Postgres.

# antes de sair executado, crie um Application no Striim e exporte o TQL para pegar o HASH da senha que você vai precisar para o campo Password


# Importando bibliotecas necessárias
import os

# Função para gerar arquivos com base no template
def gerar_arquivos(tabelas
                    , prefixo='IdentificadorQualquer' # trocar aqui pelo prefixo do cliente
                    , diretorio='um diretório na sua máquina' # caminho onde os arquivos serão gerados
                    , IP_DO_SERVIDOR_DE_BANCO_DE_ORIGEM='' # IP do servidor de origem
                    , IP_DO_SERVIDOR_DE_DESTINO='' # IP do servidor de destino
                    , BANCO_DE_ORIGEM='' # Banco de origem
                    , banco_de_destino='' # banco de destino
                    , usuario='' # usuario para conexao com o banco
                    , senha='' # hash da senha usada para conectar no banco, aquele hash do arquivo templace que voce gerou acima
                  ): 
    # Cria o diretório de saída se não existir
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)

    for i, tabela in enumerate(tabelas, start=1):
        numero = str(i).zfill(2)  # Numeração com dois dígitos
        nome_arquivo = f"{diretorio}/{prefixo}{numero}.tql"

        # Template do arquivo
        conteudo = f"""CREATE OR REPLACE APPLICATION {prefixo}{numero}; 

CREATE OR REPLACE SOURCE {prefixo}{numero} USING Global.DatabaseReader ( 
  ConnectionURL: 'jdbc:sqlserver://{IP_DO_SERVIDOR_DE_BANCO_DE_ORIGEM}:1433;DatabaseName={BANCO_DE_ORIGEM}', 
  QuiesceOnILCompletion: true, 
  adapterName: 'DatabaseReader', 
  Password_encrypted: 'true', 
  connectionProfileName: '', 
  FetchSize: 99997,
  Username: '{usuario}',
  ParallelThreads: 1, 
  Tables: '{tabela};',
  CreateSchema: false, 
  Password: '{senha}', 
  RestartBehaviourOnILInterruption: 'keepTargetTableData', 
  useConnectionProfile: false, 
  Query: '', 
  DatabaseProviderType: 'SQLServer' ) 
OUTPUT TO {prefixo}{numero}; 

CREATE OR REPLACE TARGET {prefixo}{numero}PG USING Global.DatabaseWriter ( 
  Tables: '{BANCO_DE_ORIGEM}.dbo.view2gcp_{tabela},{banco_de_destino}.public.{tabela.lower()};', 
  ConnectionRetryPolicy: 'retryInterval=30, maxRetries=3', 
  StatementCacheSize: 50, 
  BatchPolicy: 'EventCount:10000,Interval:60', 
  CommitPolicy: 'EventCount:10000,Interval:60', 
  CheckPointTable: 'CHKPOINT', 
  ParallelThreads: 1, 
  Password_encrypted: 'true', 
  PreserveSourceTransactionBoundary: false, 
  CDDLAction: 'Process',
  IgnorableExceptionCode: '22007,22009,42804,22021,23505', 
  Username: '{usuario}', 
  DatabaseProviderType: 'CloudSqlPostgres', 
  Password: '{senha}', 
  ConnectionURL: 'jdbc:postgresql://{IP_DO_SERVIDOR_DE_DESTINO}:5432/{banco_de_destino}' ) 
INPUT FROM {prefixo}{numero};

END APPLICATION {prefixo}{numero};
"""

        # Escreve o conteúdo no arquivo
        with open(nome_arquivo, 'w') as f:
            f.write(conteudo)
        print(f"Arquivo gerado: {nome_arquivo}")

# Exemplo de uso
tabelas = ['tabela1','tabela2','tabela3']

# para cada tabela em tabelas ele vai gerar um arquivo no diretório de destino, após a criação de todos, é só impotar no Striim e começar a executar o ETL.
gerar_arquivos(tabelas)
