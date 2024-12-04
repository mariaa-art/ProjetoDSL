import cmd
import pandas as pd
import json
import csv
import os
from tabulate import tabulate


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

    # Manipulação do CSV
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

    def do_GROUP_BY(self, arg):
        'GROUP_BY [coluna]: Agrupa os dados pela coluna especificada.'
        if self.data is not None:
            try:
                column = arg.strip().strip('"')
                self.data = self.data.groupby(column).sum().reset_index()
                print(f"Dados agrupados pela coluna '{column}' com sucesso.")
            except Exception as e:
                print(f"Erro ao agrupar os dados: {e}")
        else:
            print("Nenhum dado carregado. Use o comando LOAD primeiro.")

    def do_SORT_BY(self, arg):
        'SORT_BY [coluna] [ordem]: Ordena os dados de acordo com uma coluna específica.'
        if self.data is not None:
            try:
                parts = arg.split()
                column = parts[0].strip().strip('"')
                order = parts[1].strip().upper() if len(parts) > 1 else 'ASC'
                ascending = True if order == 'ASC' else False
                self.data = self.data.sort_values(by=column, ascending=ascending)
                print(f"Dados ordenados pela coluna '{column}' em ordem {'ascendente' if ascending else 'descendente'}.")
            except Exception as e:
                print(f"Erro ao ordenar os dados: {e}")
        else:
            print("Nenhum dado carregado. Use o comando LOAD primeiro.")

    def do_UPDATE(self, arg):
        'UPDATE [coluna] [novo_valor] WHERE [condição]: Atualiza valores em uma coluna com base em uma condição.'
        if self.data is not None:
            try:
                # Divida a entrada em duas partes: a parte de atualização e a condição
                update_part, condition = arg.split("WHERE")
                column, new_value_expression = update_part.strip().split("=")
                column = column.strip().strip('"').strip("'")
                condition = condition.strip().replace("==", "=").replace("'", "").replace('"', '')
                condition_column, condition_value = condition.split("=")
                condition_column = condition_column.strip()
                condition_value = condition_value.strip()

                # Verificar se a coluna existe no DataFrame
                if column in self.data.columns:
                    mask = self.data[condition_column] == condition_value
                    self.data.loc[mask, column] = pd.eval(new_value_expression)  # Avaliando a expressão do novo valor
                    print(f"Valores atualizados na coluna '{column}' onde '{condition_column} == {condition_value}'.")
                else:
                    print(f"Coluna '{column}' não encontrada.")
            except Exception as e:
                print(f"Erro ao atualizar os dados: {e}")
        else:
            print("Nenhum dado carregado. Use o comando LOAD primeiro.")

    def do_SHOW(self, arg):
        'SHOW: Exibe os dados atuais.'
        if self.data is not None:
            print(tabulate(self.data, headers='keys', tablefmt='pretty'))  # Exibindo os dados de forma tabular
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

            other_data = pd.read_csv(other_file)
            print(f"Colunas no arquivo '{other_file}': {other_data.columns.tolist()}")
            print(f"Colunas no arquivo principal: {self.data.columns.tolist()}")

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
