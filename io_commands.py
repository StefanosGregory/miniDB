from database import Database
import re

global_db = None


def ascs(cmdlist):
    asc = None
    try:
        if next(i for i, v in enumerate(cmdlist) if v.upper() == "ASC"):
            asc = True
        elif next(i for i, v in enumerate(cmdlist) if v.upper() == "DESC"):
            asc = False
    except:
        asc = None
    return asc


def fun(var):
    l1 = ','
    l2 = '='
    if var in l1 or var in l2:
        return False
    else:
        return True


def connect(dbnane):
    global_db = Database(dbnane, load=True)
    main(global_db)


def dot(cmdlist, index):
    if '.' in cmdlist[index - 1]:
        tmp = cmdlist[index - 1].split('.')
        dbname = tmp[0]
        table_name = tmp[1]
    else:
        table_name = cmdlist[index - 1]
        dbname = None
    return table_name, dbname


def find(cmdlist, lookingfor, num):
    index, tmp = 0, None
    try:
        index = next(i for i, v in enumerate(cmdlist) if v.upper() == lookingfor)
        tmp = str(cmdlist[index + num])
        return index, tmp
    except:
        return index, tmp


def enalax(cmdlist):
    tmp = cmdlist[cmdlist.index('set') + 1:cmdlist.index('where')]
    tmp = list(filter(fun, tmp))
    columns_names = []
    columns_values = []
    for s in tmp:
        columns_names.append(s.replace(',', ''))
        columns_names, columns_values = columns_values, columns_names
    return columns_values, columns_names,


def main(global_db):
    cmd = input(">>> ")
    cmdlist, parentheses = [], []
    try:
        parentheses = re.split('[()]', cmd)
        parentheses = list(filter(None, parentheses))
        cmdlist = parentheses[0]
        cmdlist = cmdlist.split(' ')
        cmdlist = list(filter(None, cmdlist))
    except:
        print("Error check your command line..")
        main(global_db)

    # <-------- SELECT SECTION -------->
    if cmdlist[0].upper() == "SELECT":
        try:
            if ('INNER' in cmdlist or 'inner' in cmdlist) and '*' in cmdlist:
                if global_db is None:
                    print("For inner join you have to load the database..")
                else:
                    table1 = find(cmdlist, 'INNER', -1)[1]
                    table2 = find(cmdlist, 'INNER', 2)[1]
                    condition = find(cmdlist, 'ON', 1)[1]

                    if 'WHERE' in cmdlist or 'where' in cmdlist:
                        condition2 = find(cmdlist, 'WHERE', 1)[1]
                        top_k = find(cmdlist, 'TOP', 2)[1]
                        order_by = find(cmdlist, 'ORDER', 2)[1]
                        asc = ascs(cmdlist)
                        global_db.inner_join(table1, table2, condition)._select_where('*', condition2, order_by, asc,
                                                                                      top_k)
                    else:
                        global_db.inner_join(table1, table2, condition)
            elif ('INNER' in cmdlist or 'inner' in cmdlist) and ('WHERE' in cmdlist or 'where' in cmdlist):
                if global_db is None:
                    print("For inner join you have to load the database..")
                else:
                    columns = cmdlist[1]
                    tmp = find(cmdlist, 'INNER', -1)
                    index = tmp[0]
                    table1 = tmp[1]
                    table2 = cmdlist[index + 2]
                    tmp = find(cmdlist, 'ON', 1)
                    index = tmp[0]
                    condition1 = tmp[1]
                    condition2 = cmdlist[index + 3]
                    top_k = find(cmdlist, 'TOP', 2)[1]
                    order_by = find(cmdlist, 'ORDER', 2)[1]
                    asc = ascs(cmdlist)

                    try:
                        global_db.inner_join(table1, table2, condition1, return_object=True)._select_where(columns,
                                                                                                           condition2,
                                                                                                           order_by,
                                                                                                           asc, top_k)
                    except:
                        print("Error check your command line..")

            else:
                if cmdlist[1].upper() == "TOP":
                    top_k = int(cmdlist[2])
                    flag_in = 3
                else:
                    flag_in = 1
                    top_k = None

                condition, index, table_name = None, 0, None
                if 'where' in cmdlist or 'WHERE' in cmdlist:
                    tmp = find(cmdlist, 'WHERE', 1)
                    index = tmp[0]
                    condition = tmp[1]
                    tmp = dot(cmdlist, index)
                    table_name = tmp[0]
                else:
                    tmp = find(cmdlist, 'FROM', 1)
                    index = tmp[0]
                    tmp = dot(cmdlist, index + 2)
                    table_name = tmp[0]

                order_by = find(cmdlist, 'ORDER', 2)[1]
                save_as = find(cmdlist, 'SAVE', 2)[1]
                asc = ascs(cmdlist)
                columns = cmdlist[flag_in:next(i for i, v in enumerate(cmdlist) if v.upper() == "FROM")]
                if columns[0] == '*':
                    columns = '*'
                else:
                    columns = columns[0].split(',')

                if tmp[1] is not None:
                    dbname = tmp[1]
                    global_db = Database(dbname, load=True)
                global_db.unlock_table(table_name)
                global_db.select(table_name, columns, condition, order_by, asc, top_k, save_as)
                if not save_as is None:
                    print('Saved..loading \n')
                    global_db.select(save_as, '*')
        except:
            print("Error check your command line..")
        main(global_db)

    # <-------- UPDATE SECTION -------->
    elif cmdlist[0].upper() == "UPDATE" and cmdlist[2].upper() == "SET":
        try:
            tmp = dot(cmdlist, 2)
            table_name = tmp[0]
            if tmp[1] is not None:
                dbname = tmp[1]
                global_db = Database(dbname, load=True)
            tmp = enalax(cmdlist)
            columns_values = tmp[0]
            columns_names = tmp[1]
            condition = parentheses[1]
            global_db.unlock_table(table_name)
            global_db.update(table_name, columns_values[0], columns_names[0], condition)
            print("Record updated successfully..")
        except:
            print("Error check your command line..")
        main(global_db)

    # <-------- INSERT SECTION -------->
    elif cmdlist[0].upper() == "INSERT" and cmdlist[1].upper() == "INTO" and cmdlist[3].upper() == "VALUES":
        try:
            tmp = dot(cmdlist, 3)
            table_name = tmp[0]
            if tmp[1] is not None:
                dbname = tmp[1]
                global_db = Database(dbname, load=True)
            row = re.split('[ ,]', parentheses[1])
            row = list(filter(None, row))
            global_db.insert(table_name, row)
            print("Record inserted successfully..")
        except:
            print("Error check your command line..")
        main(global_db)

    # <-------- DELETE SECTION -------->
    elif cmdlist[0].upper() == "DELETE" and cmdlist[1].upper() == "FROM" and cmdlist[3].upper() == "WHERE":
        try:
            tmp = dot(cmdlist, 3)
            table_name = tmp[0]
            if tmp[1] is not None:
                dbname = tmp[1]
                global_db = Database(dbname, load=True)
            conditon = parentheses[1]
            global_db.unlock_table(table_name)
            global_db.delete(table_name, conditon)
            print("Deleted successfully..")
        except:
            print("Error check your command line..")
        main(global_db)

    # <-------- CREATE SECTION -------->
    elif cmdlist[0].upper() == "CREATE":
        if cmdlist[1].upper() == "DATABASE":  # <-------- CREATE Database ------->
            Database(cmdlist[2], load=False)
            connect(cmdlist[2])
        elif cmdlist[1].upper() == "TABLE":  # <-------- CREATE Table ---------->
            try:
                par_content = re.split('[ ,]', parentheses[1])
                par_content = list(filter(None, par_content))

                primary_key = None
                if 'pk' in par_content:
                    tmp = find(par_content, 'PK', 1)
                    primary_key = find(par_content, 'PK', -2)[1]
                    par_content.pop(tmp[0])
                column_names = []
                column_types = []
                for e in par_content:
                    column_names.append(e)
                    column_names, column_types = column_types, column_names

                tmp = dot(cmdlist, 3)
                table_name = tmp[0]
                if tmp[1] is not None:
                    dbname = tmp[1]
                    global_db = Database(dbname, load=True)

                for i in range(len(column_types)):
                    if column_types[i] == 'str':
                        column_types[i] = str
                    elif column_types[i] == 'int':
                        column_types[i] = int

                global_db.create_table(table_name, column_names, column_types, primary_key)
            except IndexError:
                print("Error check your command line..")
        elif cmdlist[1].upper() == "INDEX":
            tmp = dot(cmdlist, 5)
            table_name = tmp[0]
            if tmp[1] is not None:
                dbname = tmp[1]
                global_db = Database(dbname, load=True)
            type = 'Btree'
            name = cmdlist[2]
            global_db.create_index(table_name, name, type)
        else:
            print("Error check your command line..")
        main(global_db)

    # <-------- DROP SECTION -------->
    elif cmdlist[0].upper() == "DROP":
        if cmdlist[1].upper() == "DATABASE":
            global_db = Database(cmdlist[2], load=True)
            try:
                global_db.drop_db()
                print('Database dropped..')
                global_db = None
            except:
                print("Error check your command line..")
        elif cmdlist[1].upper() == "TABLE":  # <-------- TABLE SECTION -------->
            tmp = dot(cmdlist, 3)
            table_name = tmp[0]
            if tmp[1] is not None:
                dbname = tmp[1]
                global_db = Database(dbname, load=True)
            try:
                global_db.drop_table(table_name)
                print(table_name + " droped successfully..")
            except:
                print("Error check your command line..")

                '''For future index add'''
        # elif cmdlist[1].upper() == "INDEX":
        #     tmp = dot(cmdlist, 3)
        #     indexname = tmp[0]
        #     if tmp[1] is not None:
        #         dbname = tmp[1]
        #         global_db = Database(dbname, load=True)
        #     global_db.drop_index(indexname)

        else:
            print("Error check your command line..")
        main(global_db)

    # <-------- LOAD SECTION -------->
    elif cmdlist[0].upper() == "LOAD" and cmdlist[1].upper() == "DATABASE":
        connect(cmdlist[2])

    # <-------- EXIT SECTION -------->
    elif cmdlist[0].upper() == "EXIT()" or cmdlist[0].upper() == "EXIT" or cmdlist[0].upper() == "CLOSE":
        print('Exit MiniDB..')

    else:
        print("Non correct type of command..")
        main(global_db)


main(global_db)
