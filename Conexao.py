import postgresql as pg
class Conexao(object):
    db = None
    def __init__(self, banco):
        self.db = pg.open(banco)

    def manipular(self, sql):
        try:
           self.db.execute(sql)
        except:
            return False
        return True

    def consultar(self, sql):
        rs = None
        try:
            rs = self.db.prepare(sql)
        except:
            return None
        return rs

    def proximaPK(self, tabela, chave):
        sql='select max('+chave+') from '+tabela
        rs = self.consultar(sql)
        pk = rs.first()
        return pk+1

    def fechar(self):
        self.db.close()