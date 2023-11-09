import datetime
import firebase_admin
import uuid
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import messaging
from collections import Counter

#Excluir coleções
def deletar_colletion(coll_ref, batch_size):
    '''função para deletar a collection quando identificada a necessidade'''
    docs = coll_ref.list_documents(page_size=batch_size)
    deleted = 0

    for doc in docs:
        print(f"Deleting doc {doc.id} => {doc.get().to_dict()}")
        doc.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        return deletar_colletion(coll_ref, batch_size)
    
#Realizar o envio de notificações
def enviar_notificacao(registration_token, mensagem):
    '''função para enviar notificação aos aparelhos envolvidos na alteração de preço'''
    message = messaging.Message(
        notification=messaging.Notification(
            title = "Chegou uma alteração nos preços",
            body = mensagem,
        ),
        token= registration_token,
    )
    response = messaging.send(message)
    print('Mensagem enviada', response)
    print(message)


def cria_instancia_firebase():
    '''Inicializar o client do firestore e criar instancia do banco de dados'''
    cred = credentials.Certificate(r"C:\Users\Risti\Documents\TCC\Servidor Python\Chave\postoaqui-f3273-firebase-adminsdk-kggvt-f686f021b9.json")# Substitua pelo caminho para suas próprias credenciais
    app = firebase_admin.initialize_app(cred)
    db = firestore.client()
    return db

#Acessar a lista de Postos
def retorna_collection_e_stream(db,nome,sub_colletion=None):
    '''retorna a collection e a stream'''

    if sub_colletion is None:
        colletion = db.collection(nome)
        stream = colletion.stream()

    else:
        colletion = sub_colletion.collection(nome)
        stream = colletion.stream()

    return colletion, stream

db = cria_instancia_firebase()

#Obter nome dos postos que sofreram alteração
posto_alter = []
mensagem = ''

postos_ref, postos_doc = retorna_collection_e_stream(db,'Postos')
aparelhos_ref, aparelhos_docs = retorna_collection_e_stream(db,'Aparelhos')

#Acessar todos os postos
def separa_precos_em_listas(lista_preco_gas, lista_preco_al, lista_preco_die, data_usuario):
    if 'precoGas' in data_usuario:
        valor_gas = float(data_usuario['precoGas'].replace(',', '.'))
        lista_preco_gas.append(valor_gas)
        print(lista_preco_gas)

    if 'precoAl' in data_usuario:
        valor_al = float(data_usuario['precoAl'].replace(',', '.'))
        lista_preco_al.append(valor_al)
        print(lista_preco_al)

    if 'precoDie' in data_usuario:
        valor_die = float(data_usuario['precoDie'].replace(',', '.'))
        lista_preco_die.append(valor_die)
        print(lista_preco_die)

    return lista_preco_gas, lista_preco_al, lista_preco_die

def calcula_quantidade_precos_repetidos(lista_preco_gas, lista_preco_al, lista_preco_die):
    quantidade_valores_gas = Counter(lista_preco_gas)
    quantidade_valores_al = Counter(lista_preco_al)
    quantidade_valores_die = Counter(lista_preco_die)
    print(quantidade_valores_gas)
    print(quantidade_valores_al)
    print(quantidade_valores_die)
    return quantidade_valores_gas,quantidade_valores_al,quantidade_valores_die

def retorna_preco_que_mais_repete(quantidade_valores_gas, quantidade_valores_al, quantidade_valores_die):
    valor_repetido_gas = max(quantidade_valores_gas, key=lambda x: quantidade_valores_gas[x])
    valor_repetido_al = max(quantidade_valores_al, key=lambda x: quantidade_valores_al[x])
    valor_repetido_die = max(quantidade_valores_die, key=lambda x: quantidade_valores_die[x])
    print(valor_repetido_gas)
    print(valor_repetido_al)
    print(valor_repetido_die)
    return valor_repetido_gas,valor_repetido_al,valor_repetido_die

def convert_preco_em_numerico(preco_fim):
    valorGasolina = float(preco_fim['precoGas'].replace(',', '.'))
    valorAlcool = float(preco_fim['precoAl'].replace(',', '.'))
    valorDiesel = float (preco_fim['precoDie'].replace(',', '.'))
    return valorGasolina,valorAlcool,valorDiesel

def processa_alteracoes_preco(preco_ref, precos_usuarios_ref,
                              valor_repetido_gas, valorGasolina, 
                              valor_repetido_al,valorAlcool,
                              valor_repetido_die,valorDiesel,
                              nome_posto,
                              posto):
    
    #Identificar os postos que não sofreram alterações em seus preços
    if (valor_repetido_gas == valorGasolina and valor_repetido_al == valorAlcool and valor_repetido_die == valorDiesel):
        deletar_colletion(precos_usuarios_ref, 10) 

    else: 
    #Salvar os preços final no banco de dados
        precos_finais = {
            'id': str(uuid.uuid4()),
            'dataModificacao': (datetime.date.today()).strftime('%d/%m/%y'),
            'precoGas': (str(valor_repetido_gas).replace('.', ',')),
            'precoAl': (str(valor_repetido_al).replace('.', ',')),
            'precoDie': (str(valor_repetido_die).replace('.', ','))
        }
        posto_alter = nome_posto['nome']
        if(mensagem == ''):
            mensagem = posto_alter
        
        else:
            mensagem = mensagem + ',' + posto_alter
        deletar_colletion(preco_ref, 10)
        posto.reference.collection('Precos').add(precos_finais)
        print('Preço final criado para Posto ID:', posto.id)
        deletar_colletion(precos_usuarios_ref, 10)
 
for posto in postos_doc:
#Acessar a subcoleção PrecosUsuarios de cada posto cadastrado no aplicativo 
    posto_dict = posto.to_dict()
    precos_usuarios_ref, precos_usuarios_ref = retorna_collection_e_stream(None,'PrecosUsuarios',posto)
    nome_posto = posto_dict['nome']
    print(nome_posto)

    lista_preco_gas = []
    lista_preco_al = []
    lista_preco_die = []

#Acessar os documentos da subcoleção PreçosUsuarios
    for preco in precos_usuarios_ref:
        data_usuario = preco.to_dict()

        lista_preco_gas, lista_preco_al, lista_preco_die = separa_precos_em_listas(lista_preco_gas, lista_preco_al, lista_preco_die, data_usuario)
    
        quantidade_valores_gas, quantidade_valores_al, quantidade_valores_die = calcula_quantidade_precos_repetidos(lista_preco_gas, lista_preco_al, lista_preco_die)

        valor_repetido_gas, valor_repetido_al, valor_repetido_die = retorna_preco_que_mais_repete(quantidade_valores_gas, quantidade_valores_al, quantidade_valores_die)
    
        preco_ref, preco_docs = retorna_collection_e_stream(None,'Precos',posto)

        for preco_final in preco_docs:
            preco_fim = preco_final.to_dict()

            valorGasolina, valorAlcool, valorDiesel = convert_preco_em_numerico(preco_fim)
            
            processa_alteracoes_preco(preco_ref, precos_usuarios_ref,
                                        valor_repetido_gas, valorGasolina, 
                                        valor_repetido_al,valorAlcool,
                                        valor_repetido_die,valorDiesel,
                                        nome_posto,
                                        posto)

print('Postos que sofreram alteração')
mensagem_postos = 'Acabaram de chegar alterações nos preços do(s) posto(s) ' + mensagem + ' corre para conferir'
print(mensagem_postos)

#Enviar notificações
for aparelho in aparelhos_docs:
            aparelho_doc = aparelho.to_dict()
            token = aparelho_doc.get("token")
                
            enviar_notificacao(token, mensagem_postos)
            print('Notificação enviada')

#Parar a execução
firebase_admin.delete_app(firebase_admin.get_app())

