import cmd
import pandas as pd
import json

class DSLInterpreter(cmd.Cmd):
    intro = "Bem-vindo à interface de linha de comando da DSL. Digite 'help' ou '?' para listar os comandos.\n"
    prompt = '1 > '
    file = None
    command_count = 1

    def __init__(self):
        super().__init__()
        self.data = None  
        self.project = {}  
        self.last_filter_value = None  

    def precmd(self, line):
        self.prompt = f"{self.command_count} > "
        self.command_count += 1
        return line

    # Comandos para Manipulação de CSV
    def do_LOAD(self, arg):
        'LOAD [caminho]: Carrega um arquivo CSV para manipulação.'
        try:
            path = arg.strip('"')
            self.data = pd.read_csv(path)
            print(f"Arquivo '{path}' carregado com sucesso.")
        except Exception as e:
            print(f"Erro ao carregar o arquivo: {e}")

    def do_FILTER(self, arg):
        'FILTER [coluna] [operador] [valor]: Filtra linhas com base em uma condição.'
        if self.data is not None:
            try:
                for operator in ['>=', '<=', '!=', '=', '>', '<']:
                    if operator in arg:
                        column, value = arg.split(operator, 1)
                        column = column.strip().strip('"').strip("'")
                        value = value.strip().strip('"').strip("'")
                        
                        if operator == '=':
                            operator = '=='

                        if value.isdigit() or value.replace('.', '', 1).isdigit():
                            query_str = f"{column} {operator} {value}"
                        else:
                            query_str = f"{column} {operator} '{value}'"
                        break
                else:
                    raise ValueError("Operador inválido. Use =, >, <, >=, <= ou !=.")
                
                self.data = self.data.query(query_str)
                self.last_filter_value = value 
                print("Dados filtrados com sucesso.")
            except Exception as e:
                print(f"Erro ao filtrar os dados: {e}")
        else:
            print("Nenhum dado carregado. Use o comando LOAD primeiro.")

    def do_SELECT(self, arg):
        'SELECT [colunas]: Seleciona colunas específicas.'
        if self.data is not None:
            try:
                columns = [col.strip().strip('"').strip("'") for col in arg.split(',')]
                self.data = self.data[columns]
                print("Colunas selecionadas com sucesso.")
            except KeyError as e:
                print(f"Erro ao selecionar colunas: {e}")
            except Exception as e:
                print(f"Erro inesperado: {e}")
        else:
            print("Nenhum dado carregado. Use o comando LOAD primeiro.")

    def do_SHOW(self, arg):
        'SHOW: Exibe os dados atuais.'
        if self.data is not None:
            print(self.data)
        else:
            print("Nenhum dado para exibir.")

    def do_SAVE(self, arg):
        'SAVE [caminho]: Salva os dados atuais em um arquivo CSV.'
        if self.data is not None:
            try:
                path = arg.strip('"')
                self.data.to_csv(path, index=False)
                print(f"Dados salvos em '{path}' com sucesso.")
            except Exception as e:
                print(f"Erro ao salvar os dados: {e}")
        else:
            print("Nenhum dado para salvar.")

    def do_JOIN(self, arg):
        'JOIN [outro_arquivo] ON [coluna]: Realiza um join com outro arquivo CSV baseado em uma coluna comum.'
        try:
            parts = arg.split('ON')
            if len(parts) != 2:
                print("Sintaxe incorreta. Use: JOIN [outro_arquivo] ON [coluna]")
                return

            other_file = parts[0].strip().strip('"')
            join_column = parts[1].strip().strip('"')

            # Carrega o segundo arquivo e mostra as colunas para verificação
            other_data = pd.read_csv(other_file)
            print(f"Colunas no arquivo '{other_file}': {other_data.columns.tolist()}")
            print(f"Colunas no arquivo principal: {self.data.columns.tolist()}")

            # Realiza o join
            self.data = pd.merge(self.data, other_data, on=join_column, how='inner')
            print(f"Join com '{other_file}' realizado com sucesso na coluna '{join_column}'.")
        except Exception as e:
            print(f"Erro ao realizar o join: {e}")

    def do_REMOVE_DUPLICATES(self, arg):
        'REMOVE_DUPLICATES [coluna]: Remove linhas duplicadas com base em uma coluna.'
        if self.data is not None:
            try:
                column = arg.strip('"')
                self.data = self.data.drop_duplicates(subset=column)
                print(f"Linhas duplicadas removidas com base na coluna '{column}'.")
            except Exception as e:
                print(f"Erro ao remover duplicatas: {e}")
        else:
            print("Nenhum dado carregado. Use o comando LOAD primeiro.")

    def do_EXPORT_JSON(self, arg):
        'EXPORT_JSON [caminho]: Exporta os dados para um arquivo JSON.'
        if self.data is not None:
            try:
                if not arg.strip():
                    if self.last_filter_value:
                        filename = f"vendas_{self.last_filter_value.lower()}.json"
                    else:
                        filename = "dados_exportados.json"
                else:
                    filename = arg.strip('"')
                
                self.data.to_json(filename, orient='records')
                print(f"Dados exportados para '{filename}' com sucesso.")
            except Exception as e:
                print(f"Erro ao exportar os dados: {e}")
        else:
            print("Nenhum dado para exportar.")

    def do_EXIT(self, arg):
        'EXIT: Sai da interface de linha de comando.'
        print("Saindo da interface DSL.")
        return True

    def do_EOF(self, arg):
        return self.do_EXIT(arg)

if __name__ == '__main__':
    DSLInterpreter().cmdloop()
