import pandas
import psycopg2
import psycopg2.extras
from tkinter import Tk
from tkinter.filedialog import askopenfilename

Tk().withdraw() 
conn = psycopg2.connect('dbname=cloffa host= user= password=')
source_excel=askopenfilename() #"D:\\DEV\\FAUNAFRI\\rework_biblio_cloffa.xls"

dict_taxon={}
dict_author={}
dict_ref={}
dict_description={}

def coalesce(s,d):
    if s is None:
        return d
    else:
        return s
        
def get_rank_id(rank):
    if rank=="genus":
        return 17
    elif rank=="subgenus":
        return 18
    elif rank=="species":
        return 19
    elif rank=="subspecies":
        return 20
    else:
        return None
        
def find_next(table_name, field_name, comparator="MAX"):
    max_id=None
    cur1 = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur1.execute("SELECT "+comparator+"("+field_name+") as max_id FROM "+table_name)
    tmp1=cur1.fetchall()
    cur1.close()
    if len(tmp1)>0:
        max_id=tmp1[0]["max_id"]
        max_id=max_id+1
    return max_id
    
def find_taxon(name, rank, status):
    print("NAME="+str(name))
    print("RANK="+str(rank))
    print("STATUS="+str(status))
    rank_id= get_rank_id(rank)
    print("RANK_ID="+str(rank_id))
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    if rank_id==19:
        cur.execute("SELECT * FROM v_taxon_full_name_recursive WHERE full_taxon_name=%s and idrang=%s and idflag=%s ORDER BY idtaxon", (name,rank_id, status))
    else:
        cur.execute("SELECT * FROM taxon WHERE nomtaxon=%s and idrang=%s and idflag=%s", (name,rank_id, status))
    tmp=cur.fetchall()
    cur.close()
    return tmp
    
def find_author(surname,first_name=None):
    print(surname)
    print(first_name)
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    if first_name is not None:
        cur.execute("SELECT * FROM auteur WHERE nomauteur=%s AND prenomauteur=%s", (surname.strip(), first_name.strip()))
    else:
        cur.execute("SELECT * FROM auteur WHERE nomauteur=%s ", (surname.strip(),))
    tmp=cur.fetchall()    
    cur.close()
    return tmp
    
def find_generic(table_name, list_sql_fields, list_variables):
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    array_sql=[x+ " = %s" for x in list_sql_fields]
    sql="SELECT * FROM "+ table_name + " WHERE "+ " AND ".join(array_sql) 
    cur.execute(sql, list_variables )
    tmp=cur.fetchall()    
    cur.close()
    return tmp 
    
def insert_taxon(name, rank, status, idparent):
    max_id=None
    rank_id= get_rank_id(rank)
    if rank_id is not None:        
        max_id=find_next("taxon", "idtaxon")
        max_gauche=find_next("taxon", "max(gauche), max(droit)", "GREATEST")
        if max_id is not None and  max_gauche is not None :            
            cur1 =  conn.cursor()           
            cur1.execute("INSERT INTO taxon (idtaxon, nomtaxon, idrang, apourpere, gauche, droit, idflag) VALUES(%s, %s, %s, %s, %s, %s, %s)", (max_id, name, rank_id, idparent,max_gauche, max_gauche+1, status))
            conn.commit()
            cur1.close()
        else:
            print("ERROR can't get taxonid")
    print(max_id)
    return max_id

def insert_author(surname,first_name):
    print(surname)
    print(first_name)
    test_name_only=find_author(surname)
    if len(test_name_only)>0:
        name_only_id=test_name_only[0]["idauteur"]
    else:
        max_id=find_next("auteur", "idauteur")
        cur1 =  conn.cursor()
        cur1.execute("INSERT INTO auteur (idauteur, nomauteur) VALUES (%s, %s)", (max_id, surname))
        cur1.close()
    max_id=find_next("auteur", "idauteur")
    cur2 =  conn.cursor()
    cur2.execute("INSERT INTO auteur (idauteur, nomauteur, prenomauteur) VALUES (%s, %s, %s)", (max_id, surname, first_name))
    conn.commit()    
    cur2.close()
    return max_id
    


def insert_generic(table_name, list_fields, list_variables, key_field=None):
    if key_field is None:
        cur1 =  conn.cursor()
        list_fields_str=",".join(list_fields)
        list_values= ["%s"] * len(list_fields)
        list_values_str=",".join(list_values)
        cur1.execute("INSERT INTO "+ table_name +" ("+ list_fields_str +") VALUES (" +list_values_str +")", list_variables)
        cur1.close()
        conn.commit()
        return None
    else:
        max_id=find_next(table_name, key_field)
        cur1 =  conn.cursor()
        print(list_fields)
        list_fields.insert(0, key_field)
        list_variables.insert(0, str(max_id))
        list_values= ["%s"] * len(list_fields)
        list_fields_str=",".join(list_fields)
        list_values_str=",".join(list_values)
        cur1.execute("INSERT INTO "+ table_name +" ("+ list_fields_str +") VALUES (" +list_values_str +")", list_variables)        
        cur1.close()
        conn.commit()
        return max_id
    
##main
xls = pandas.ExcelFile(source_excel)
df_ref = pandas.read_excel(xls, "reference")
df_authors = pandas.read_excel(xls, "authors")
df_decritpar = pandas.read_excel(xls, "decritpar")
df_link_bib_taxa = pandas.read_excel(xls, "link_bib_taxa")
df_data_origin = pandas.read_excel(xls, "data_origin")
df_attributes = pandas.read_excel(xls, "attributes")
df_properties = pandas.read_excel(xls, "proprietes")
df_aliasde = pandas.read_excel(xls, "aliasde")
df_dataorigin = pandas.read_excel(xls, "data_origin")
df_data_species_origin = pandas.read_excel(xls, "data_species_origin")

df_ref=df_ref.fillna("")
df_authors=df_authors.fillna("")
df_decritpar=df_decritpar.fillna("")
df_link_bib_taxa=df_link_bib_taxa.fillna("")
df_data_origin=df_data_origin.fillna("")
df_attributes=df_attributes.fillna("")
df_properties=df_properties.fillna("")
df_aliasde=df_aliasde.fillna("")
df_dataorigin=df_dataorigin.fillna("")
df_data_species_origin=df_data_species_origin.fillna("")

print(df_link_bib_taxa)


#taxa
print("TAXA0")
for idex, row in df_link_bib_taxa.iterrows():
    print(row)
    print(row["species"])
    rank="genus"
    local_article_id=row["id_reference_in_batch_file"]
    local_taxon_id=row["id_taxon_in_batch_file"]
    if len(row["species"].strip())>0 and len(row["genus"].strip())>0:
        sc_name=row["genus"].strip()+" "+row["species"].strip()
        rank="species"
    elif len(row["genus"].strip())>0:
        sc_name=row["genus"].strip()
        rank="genus"
    tax_ctrl=find_taxon(sc_name, rank, row["idflag"])
    print(tax_ctrl)
    if len(tax_ctrl)>0:
        print("TAXON_EXISTS !")
        for pg_row in tax_ctrl:
            print("debug row")
            print(pg_row["idtaxon"])
            species_id=pg_row["idtaxon"]
    else:
        print("NEW_TAXON")
        #test genus
        print(row["genus"])
        tax_ctrl_genus=find_taxon(row["genus"].strip(), "genus", row["idflag"])
        print(tax_ctrl_genus)
        if len(tax_ctrl_genus)>0:
            id_genus=tax_ctrl_genus[0]["idtaxon"]
            print(id_genus)
            species_id= insert_taxon(row["species"].strip(), "species", row["idflag"], id_genus)
        else:
            print("can't insert species")
    print("SPECIES_ID")
    print(species_id)
    '''
    if not local_article_id in  dict_taxon:
        dict_taxon[local_article_id]=[]
    dict_taxon[local_article_id].append(species_id)
    print(dict_taxon)
    '''
    dict_taxon[local_taxon_id]=species_id
    
#authors
print("AUTHORS")
for idex, row in df_authors.iterrows():
    print(row)
    id_auth=find_author(row["nomauteur"].strip(),row["prenomauteur"].strip())
    local_article_id=row["id_reference_in_batch_file"]
    print(id_auth)
    if len(id_auth)>0:
        key_author=id_auth[0]["idauteur"]
    else:
        print("INSERT_AUTHOR")
        key_author=insert_author(row["nomauteur"].strip(),row["prenomauteur"].strip())
    print(key_author)
    if not local_article_id in  dict_author:
        dict_author[local_article_id]={}
    dict_author[local_article_id][row["rang"]]=key_author
    
#reference
print("REFERENCE")  
for idex, row in df_ref.iterrows():
    print(row)
    local_article_id=row["id_reference_in_batch_file"]
    ref_ctrl= find_generic("biblio", ("titrebiblio", "anneepublication", "pages", "idsupport" ,"commentaire", "extension", "editeurbiblio" ),(row["titrebiblio"].strip(), row["anneepublication"], row["pages"].strip(), row["idsupport"] ,row["commentaire"].strip(), row["extension"].strip(), row["editeurbiblio"].strip())) 
    if len(ref_ctrl)>0:
        id_ref=ref_ctrl[0]["idbiblio"]
    else:
        id_ref=insert_generic("biblio", ["titrebiblio", "editeurbiblio","anneepublication", "extension", "pages", "commentaire", "idsupport"], [row["titrebiblio"].strip(), row["editeurbiblio"].strip(), row["anneepublication"], row["extension"].strip(), row["pages"].strip(), row["commentaire"].strip(), row["idsupport"]], "idbiblio")
    dict_ref[local_article_id]=id_ref
    print(id_ref)
 
print("AUTHORS")       
for key, val in dict_author.items():
    idbiblio=dict_ref[key]
    for key2, val2 in val.items():
        rangauteur=key2
        idauteur=val2
        print(idbiblio)
        print(rangauteur)
        print(idauteur)
        id_ecritpar=find_generic("ecritpar", ("idbiblio", "idauteur", "rangauteur"), (idbiblio, idauteur, rangauteur) ) 
        if len(id_ecritpar)==0:
            print("INSERT")
            insert_generic("ecritpar",["idbiblio", "idauteur", "rangauteur"], [idbiblio, idauteur, rangauteur] )

print("-----------------DECRIT PAR----------------------------")
print("DESCRIPTION")                
for key, row in df_decritpar.iterrows():
    print(key)
    print(row)
    key_ref=dict_ref[row["id_reference_in_batch_file"]]
    key_taxon=dict_taxon[row["id_taxon_in_batch_file"]]
    print(key_ref)
    print(key_taxon)
    if not key_ref in dict_description:
        dict_description[key_ref]={}
    #useless ?
    #dict_description[key_ref][key_taxon]={}
    get_taxon=find_generic("taxon", ("idtaxon",), (key_taxon,))
    print(get_taxon)
    id_flag=1
    if len(get_taxon)>0:
        id_flag=get_taxon[0]["idflag"]
        print(id_flag)
    if id_flag==1:
        id_decritpar=find_generic("decritpar", ("idtaxon", "idbiblio"), (key_taxon, key_ref))
        print(id_decritpar)
        if len(id_decritpar)==0:
            print("INSERT")
            dereference=None
            print(str(row["dereference"]))
            if str(row["dereference"]).lower()=="false":
                dereference=False
            parenthese=None
            if str(row["parenthese"]).lower()=="false":
                parenthese=False
            print((key_taxon, key_ref,dereference,parenthese, str(row["page"]).strip() ))
            insert_generic("decritpar", ["idtaxon", "idbiblio", "dereference", "parenthese", "page"], [key_taxon, key_ref, dereference,parenthese, str(row["page"]).strip() ])
        else:
            print("LA_PUBLICATION_EXISTE")
    else:
        print("ALIAS")
        print("taxa "+str(row["id_taxon_in_batch_file"])+ " (local key)/" + str(key_taxon)+ "(database key) is an alias=> check exists in aliasde" )
        
print("-----------------------------------------------------")
#alias/synonymies
print("ALIAS_SYNONYMIES")    
for key, row in df_aliasde.iterrows():
    print(key)
    print(row)
    key_ref=""
    key_taxon=""
    key_alias=""
    if row["id_taxon_in_batch_file"] in dict_taxon:
        key_taxon=dict_taxon[row["id_taxon_in_batch_file"]]
    else:
        print("ERROR local taxon not found "+str(row["id_taxon_in_batch_file"]))
        continue
    if row["aliasde"] in dict_taxon:
        key_alias=dict_taxon[row["aliasde"]]
    else:
        print("ERROR local taxon (for synonymy alias) not found "+str(row["aliasde"]))
        continue
    if row["id_reference_in_batch_file"] in dict_ref:
        key_ref=dict_ref[row["id_reference_in_batch_file"]]
    else:
        print("ERROR local publication not found "+str(row["id_taxon_in_batch_file"]))
        continue       
    #check if synonymy exists
    alias_ctrl= find_generic("aliasde", ("idtaxon", "idflag", "aliasde", "idbiblio" ,"page" ),(key_taxon, row["idflag"], key_alias,key_ref, str(row["page"]).strip() ))
    if len(alias_ctrl)==0:
        print("INSERT_SYNONYMY")
        insert_generic("aliasde", ["idtaxon", "idflag", "aliasde", "idbiblio" ,"page"], [key_taxon, row["idflag"], key_alias,key_ref, str(row["page"]).strip() ])
 
print("PROPERTIES") 
for key, row in df_properties.iterrows():
    print(key)
    print(row)
    key_ref=dict_ref[row["id_reference_in_batch_file"]]
    key_taxon=dict_taxon[row["id_taxon_in_batch_file"]]
    print(key_ref)
    print(key_taxon)
    get_taxon=find_generic("alapropriete", ("idtaxon","idbiblio", "idattribut"), (key_taxon,key_ref, row["idattribut"]))
    if len(get_taxon)==0:
        print("INSERT PROP")
        insert_generic("alapropriete",["idtaxon","idbiblio", "idattribut", "valeurattribut"],[key_taxon,key_ref, row["idattribut"], row["valeurattribut"].strip()])
    else:
        print("ALIAS ALREADY EXISTS")

#log biblio
print("LOG_BIBLIO")
print(dict_description)
print(dict_ref)
for key, row in df_dataorigin.iterrows():
    if row["id_reference_in_batch_file"] in dict_ref:
        key_ref=dict_ref[row["id_reference_in_batch_file"]]
    else:
        print("ERROR local publication not found "+str(row["id_reference_in_batch_file"]))
        continue
    get_log=find_generic("insertbibliopar", ("idbiblio",), (key_ref,))
    if len(get_log)==0:
        print("INSERT LOG REF")
        get_people=find_generic("personne", ("LOWER(nom)", "LOWER(prenom)"), (row["name"].strip().lower(), row["surname"].strip().lower()))
        if len(get_people)==0:
            print("HAS TO CREATE PERSON "+ row["name"].strip()+ " "+row["surname"].strip())
            key_people=insert_generic("personne", ("nom", "prenom"), (row["name"].strip(),row["surname"].strip() ) , "idpersonne")
        else:
            key_people=get_people[0]["idpersonne"]
        insert_generic("insertbibliopar", ("idpersonne", "idbiblio", "dateinsertion"), (key_people, key_ref, row["date"]))
    else:
        print("LOGREF ALREADY EXISTS")

#log taxo
print("LOG_TAXO")
for key, row in df_data_species_origin.iterrows():
    key_taxon=""
    if row["id_taxon_in_batch_file"] in dict_taxon:
        key_taxon=dict_taxon[row["id_taxon_in_batch_file"]]
    else:
        print("ERROR local taxon not found "+str(row["id_taxon_in_batch_file"]))
        continue
    get_log=find_generic("inserttaxonpar", ("idtaxon",), (key_taxon,))        
    if len(get_log)==0:
        print("INSERT LOG REF")
        get_people=find_generic("personne", ("LOWER(nom)", "LOWER(prenom)"), (row["name"].strip().lower(), row["surname"].strip().lower()))
        if len(get_people)==0:
            print("HAS TO CREATE PERSON "+ row["name"].strip()+ " "+row["surname"].strip())
            key_people=insert_generic("personne", ("nom", "prenom"), (row["name"].strip(),row["surname"].strip() ) , "idpersonne")
        else:
            key_people=get_people[0]["idpersonne"]
        insert_generic("inserttaxonpar", ("idpersonne", "idtaxon", "dateinsertion"), (key_people, key_taxon, row["date"]))
    else:
        print("LOGTAXON ALREADY EXISTS")
            