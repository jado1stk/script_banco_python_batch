#conexão com o banco (O modulo necessario já deve vir junto com o batch)
from pymongo import MongoClient
client = MongoClient('mongodb://teste:teste@ds249415.mlab.com:49415/banco_teste2')
db = client.banco_teste2


#declaração de banco para auto incremento
db.orgid_counter.insert({'_id': "userid", 'seq': 1500})

#declaração função de auto incremento
def getNextSequence(collection,name):  
   return collection.find_and_modify(query= { '_id': name },update= { '$inc': {'seq': 1}}, new=True ).get('seq');


#inserção de dados (20, todos diferentes)
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "00000000000", 'nm_customer':"Jose da Silva",'is_active':"sim",'vl_total':3200})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "44321232512", 'nm_customer':"Pedro da Silva",'is_active':"sim",'vl_total':2000})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "00000000001", 'nm_customer':"Joao da Silva",'is_active':"sim",'vl_total':500})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "29358198230", 'nm_customer':"Lucas da Silva",'is_active':"sim",'vl_total':300})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "77744433322", 'nm_customer':"Jose da Silva",'is_active':"sim",'vl_total':3000})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "11234567890", 'nm_customer':"Carlos da Silva",'is_active':"sim",'vl_total':150})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "79283912730", 'nm_customer':"Marcos da Silva",'is_active':"sim",'vl_total':15000})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "32121236903", 'nm_customer':"Josue da Silva",'is_active':"sim",'vl_total':150})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "91873829010", 'nm_customer':"Adilson da Silva",'is_active':"sim",'vl_total':2050})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "29190285910", 'nm_customer':"Maria da Silva",'is_active':"sim",'vl_total':2100})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "92901203801", 'nm_customer':"Mario da Silva",'is_active':"sim",'vl_total':2500})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "21512351666", 'nm_customer':"Josefina da Silva",'is_active':"sim",'vl_total':200})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "91230102398", 'nm_customer':"Andrew da Silva",'is_active':"sim",'vl_total':100})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "89123879128", 'nm_customer':"Jefferson da Silva",'is_active':"sim",'vl_total':900})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "61236567122", 'nm_customer':"Romulo da Silva",'is_active':"sim",'vl_total':800})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "36541232333", 'nm_customer':"Alan da Silva",'is_active':"sim",'vl_total':700})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "11111111111", 'nm_customer':"Marcelo da Silva",'is_active':"sim",'vl_total':600})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "89123781237", 'nm_customer':"Marcela da Silva",'is_active':"sim",'vl_total':1100})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "91293757189", 'nm_customer':"William da Silva",'is_active':"sim",'vl_total':1700})
db.tb_customer_account.insert({'id_customer': getNextSequence(db.orgid_counter,"userid"), 'cpf_cnpj': "89128373781", 'nm_customer':"Silva da Silva",'is_active':"sim",'vl_total':1500})

#Execução destes 3 comandos no shell do Python
#1o comando
pipeline = [{'$match' :
         {'$and':
          [ { 'id_customer': { '$gt':1500 },  'id_customer': { '$lt':2700}, 'vl_total': { '$gt':560 } } ] } },
    {'$group':{
            "_id": "null",
            "media final": { "$avg": { "$sum": "$vl_total" } } } }]

#2o comando
db.command('aggregate','data',pipeline=pipeline)
#3o comando
list(db.tb_customer_account.aggregate(pipeline))
            
#Lista de clientes utilizados para conta        
procurar = db.customer_account.find( { "$and": [ { 'id_customer': { '$gt':1500 },  'id_customer': { '$lt':2700}, 'vl_total': { '$gt':560 } } ] } )    
                    
