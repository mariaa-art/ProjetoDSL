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

    # Parte 3 - Comandos para Gestão de Projetos
    def do_CREATE_PROJECT(self, arg):
        'CREATE_PROJECT [nome]: Cria um novo projeto.'
        name = arg.strip('"')
        self.project = {
            'name': name,
            'tasks': []
        }
        print(f"Projeto '{name}' criado com sucesso.")

    def do_ADD_TASK(self, arg):
        'ADD_TASK [nome] [prazo] [prioridade]: Adiciona uma tarefa ao projeto.'
        try:
            parts = arg.split('"')
            name = parts[1]
            deadline = parts[3]
            priority = parts[5]
            task = {
                'name': name,
                'deadline': deadline,
                'priority': priority,
                'assigned_to': None,
                'status': 'Não iniciada',
                'dependencies': []
            }
            self.project['tasks'].append(task)
            print(f"Tarefa '{name}' adicionada com sucesso.")
        except Exception as e:
            print(f"Erro ao adicionar tarefa: {e}")

    def do_ASSIGN(self, arg):
        'ASSIGN [tarefa] [responsável]: Atribui um responsável a uma tarefa.'
        try:
            parts = arg.split('"')
            task_name = parts[1]
            person = parts[3]
            for task in self.project['tasks']:
                if task['name'] == task_name:
                    task['assigned_to'] = person
                    print(f"Tarefa '{task_name}' atribuída a '{person}'.")
                    return
            print(f"Tarefa '{task_name}' não encontrada.")
        except Exception as e:
            print(f"Erro ao atribuir tarefa: {e}")

    def do_SET_STATUS(self, arg):
        'SET_STATUS [tarefa] [status]: Define o status de uma tarefa.'
        try:
            parts = arg.split('"')
            task_name = parts[1]
            status = parts[3]
            for task in self.project['tasks']:
                if task['name'] == task_name:
                    task['status'] = status
                    print(f"Status da tarefa '{task_name}' atualizado para '{status}'.")
                    return
            print(f"Tarefa '{task_name}' não encontrada.")
        except Exception as e:
            print(f"Erro ao definir o status da tarefa: {e}")

    def do_EXPORT_REPORT(self, arg):
        'EXPORT_REPORT [formato] [caminho]: Exporta um relatório do projeto em PDF ou CSV.'
        try:
            parts = arg.split('"')
            format = parts[1].lower()
            path = parts[3]
            if format == 'csv':
                self.export_project_csv(path)
            elif format == 'pdf':
                self.export_project_pdf(path)
            else:
                print("Formato não suportado. Use 'PDF' ou 'CSV'.")
        except Exception as e:
            print(f"Erro ao exportar relatório: {e}")

    def export_project_csv(self, path):
        try:
            df = pd.DataFrame(self.project['tasks'])
            df.to_csv(path, index=False)
            print(f"Relatório do projeto exportado para '{path}' com sucesso.")
        except Exception as e:
            print(f"Erro ao exportar CSV: {e}")

    def export_project_pdf(self, path):
        try:
            from fpdf import FPDF
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            pdf.cell(200, 10, txt=f"Projeto: {self.project['name']}", ln=True)
            for task in self.project['tasks']:
                pdf.cell(200, 10, txt=f"Tarefa: {task['name']}", ln=True)
                pdf.cell(200, 10, txt=f"Prazo: {task['deadline']}", ln=True)
                pdf.cell(200, 10, txt=f"Prioridade: {task['priority']}", ln=True)
                pdf.cell(200, 10, txt=f"Atribuído a: {task['assigned_to']}", ln=True)
                pdf.cell(200, 10, txt=f"Status: {task['status']}", ln=True)
                pdf.ln()
            pdf.output(path)
            print(f"Relatório do projeto exportado para '{path}' com sucesso.")
        except ImportError:
            print("Biblioteca FPDF não instalada. Instale-a usando 'pip install fpdf'.")
        except Exception as e:
            print(f"Erro ao exportar PDF: {e}")

    # Comando para Sair
    def do_EXIT(self, arg):
        'EXIT: Sai da interface de linha de comando.'
        print("Saindo da interface DSL.")
        return True

    def do_EOF(self, arg):
        return self.do_EXIT(arg)

if __name__ == '__main__':
    DSLInterpreter().cmdloop()
