import pymysql

# wrapper of print for tidy output
def form_print(str, total = 10, newLine = False):
    tab_size = total - len(str)
    if tab_size < 0:
        tab_size = 0
    str = str + (' '*tab_size)
    if newLine:
        print(str)
    else:
        print(str, end="")

# print line
def line_print():
    print('-------------------------------------------------------------------------------------------------')

# print blank line
def blank_print():
    print('')

# need python version >= 3.6 for f-string
class Data(object):
    def __init__(self, host='astronaut.snu.ac.kr', port=3306, student_id='20DB_2017_10488'):
        # Connect to database and make cursor
        self.db = pymysql.connect(
            host=host,
            port=3306,
            user=student_id,
            passwd=student_id,
            db=student_id,
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.db.cursor()

    def __del__(self):
        # Close database connection
        self.db.close()
        self.cursor.close()

    def is_hall_exist(self, id):
        sql = 'select * from hall where id = {};'.format(id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        self.db.commit()
        return (len(result) != 0)

    def is_concert_exist(self, id):
        sql = 'select * from concert where id = {};'.format(id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        self.db.commit()
        return (len(result) != 0)

    def is_audience_exist(self, id):
        sql = 'select * from audience where id = {};'.format(id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        self.db.commit()
        return (len(result) != 0)

    # if concert_id is not assigned, ()
    # else, (assign_id, hall_id)
    def get_assign_info(self, concert_id):
        sql = 'select id, hall_id from assign where concert_id = {};'.format(concert_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        self.db.commit()
        if len(result) == 0:
            return ()
        return (result[0]['id'], result[0]['hall_id'])

    def get_capacity(self, hall_id):
        sql = 'select capacity from hall where id = {};'.format(hall_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        self.db.commit()
        if len(result) == 0:
            print('error: trying to get capacity of non-existing hall')
            return 0
        return result[0]['capacity']

    def get_age(self, audience_id):
        sql = 'select age from audience where id = {};'.format(audience_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        self.db.commit()
        if len(result) == 0:
            print('error: trying to get age of non-existing audience')
            return 0
        return result[0]['age']

    def get_price(self, concert_id):
        sql = 'select price from concert where id = {};'.format(concert_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        self.db.commit()
        if len(result) == 0:
            print('error: trying to get price of non-existing concert')
            return 0
        return result[0]['price']

    # e.g. [1, 3, 4, 10, 15] <- seat 1,3,4,10,15 are already booked by someone
    def get_booked_seat_number(self, concert_id):
        sql = '''
            select book.seat_number
            from assign, book
            where assign.concert_id = {} and
            assign.id = book.assign_id
            order by book.seat_number
        '''.format(concert_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        self.db.commit()
        return [i['seat_number'] for i in result]


    # 1
    def show_all_hall(self):
        sql = '''
            select hall.id, hall.name, hall.location, hall.capacity, count(assign.id)
            from hall left outer join assign on (hall.id = assign.hall_id)
            group by hall.id
            order by hall.id;
        '''
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        line_print()
        form_print('id')
        form_print('name',30)
        form_print('location', 20)
        form_print('capacity', 20)
        form_print('assigned', newLine=True)
        line_print()
        for i in result:
            form_print(str(i['id']))
            form_print(i['name'],30)
            form_print(i['location'], 20)
            form_print(str(i['capacity']), 20)
            form_print(str(i['count(assign.id)']), newLine=True)
        line_print()
        blank_print()
        self.db.commit()

    # 2
    def show_all_concert(self):
        sql = '''
            select concert.id, concert.name, concert.type, concert.price, count(book.id)
            from concert left outer join assign on (concert.id = assign.concert_id) left outer join book on(assign.id = book.assign_id)
            group by concert.id
            order by concert.id;
        '''
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        line_print()
        form_print('id')
        form_print('name',30)
        form_print('type', 20)
        form_print('price', 20)
        form_print('booked', newLine=True)
        line_print()
        for i in result:
            form_print(str(i['id']))
            form_print(i['name'],30)
            form_print(i['type'], 20)
            form_print(str(i['price']), 20)
            form_print(str(i['count(book.id)']), newLine=True)
        line_print()
        blank_print()
        self.db.commit()

    # 3
    def show_all_audience(self):
        sql = '''
            select id, name, gender, age
            from audience
            order by id;
        '''
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        line_print()
        form_print('id')
        form_print('name',30)
        form_print('gender', 20)
        form_print('age', newLine=True)
        line_print()
        for i in result:
            form_print(str(i['id']))
            form_print(i['name'],30)
            form_print(i['gender'], 20)
            form_print(str(i['age']), newLine=True)
        line_print()
        blank_print()
        self.db.commit()

    # 4
    def insert_hall(self, name, location, capacity):
        sql = 'insert into hall values (default, "{}", "{}", {});'.format(name, location, capacity)
        self.cursor.execute(sql)
        self.db.commit()

    # 5
    def delete_hall(self, id):
        sql = 'delete from hall where id = {};'.format(id)
        self.cursor.execute(sql)
        self.db.commit()
    
    # 6
    def insert_concert(self, name, type, price):
        sql = 'insert into concert values (default, "{}", "{}", {});'.format(name, type, price)
        self.cursor.execute(sql)
        self.db.commit()
    
    # 7
    def delete_concert(self, id):
        sql = 'delete from concert where id = {};'.format(id)
        self.cursor.execute(sql)
        self.db.commit()

    # 8
    def insert_audience(self, name, gender, age):
        sql = 'insert into audience values (default, "{}", "{}", {});'.format(name, gender, age)
        self.cursor.execute(sql)
        self.db.commit()

    # 9
    def delete_audience(self, id):
        sql = 'delete from audience where id = {};'.format(id)
        self.cursor.execute(sql)
        self.db.commit()

    # 10
    def assign(self, hall_id, concert_id):
        sql = 'insert into assign values (default, {}, {});'.format(hall_id, concert_id)
        self.cursor.execute(sql)
        self.db.commit()
    
    # 11
    def book(self, assign_id, audience_id, seat_number):
        sql = 'insert into book values(default, {}, {}, {});'.format(assign_id, audience_id, seat_number)
        self.cursor.execute(sql)
        self.db.commit()

    # 12
    def show_assigned_concert(self, hall_id):
        sql = '''
            select concert.id, concert.name, concert.type, concert.price, count(book.id)
            from concert join assign on (concert.id = assign.concert_id) left outer join book on(assign.id = book.assign_id)
            where assign.hall_id = {}
            group by concert.id
            order by concert.id;
        '''.format(hall_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        line_print()
        form_print('id')
        form_print('name',30)
        form_print('type', 20)
        form_print('price', 20)
        form_print('booked', newLine=True)
        line_print()
        for i in result:
            form_print(str(i['id']))
            form_print(i['name'],30)
            form_print(i['type'], 20)
            form_print(str(i['price']), 20)
            form_print(str(i['count(book.id)']), newLine=True)
        line_print()
        blank_print()
        self.db.commit()

    # 13
    def show_booked_audience(self, concert_id):
        sql = '''
            select distinct audience.id, audience.name, audience.gender, audience.age
            from assign, book, audience
            where assign.concert_id = {} and
            assign.id = book.assign_id and
            book.audience_id = audience.id
            order by audience.id
        '''.format(concert_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        line_print()
        form_print('id')
        form_print('name',30)
        form_print('gender', 20)
        form_print('age', newLine=True)
        line_print()
        for i in result:
            form_print(str(i['id']))
            form_print(i['name'],30)
            form_print(i['gender'], 20)
            form_print(str(i['age']), newLine=True)
        line_print()
        blank_print()
        self.db.commit()

    # 14
    def show_seat_state(self, concert_id):
        assign_info = self.get_assign_info(concert_id)
        hall_id = assign_info[1]
        capacity = self.get_capacity(hall_id)
        sql = '''
            select book.seat_number, book.audience_id
            from assign, book
            where assign.concert_id = {} and
            assign.id = book.assign_id
            order by book.seat_number
        '''.format(concert_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        line_print()
        form_print('seat_number', 30)
        form_print('audience_id', 30, True)
        line_print()
        count = 0
        booked_seat_numbers = self.get_booked_seat_number(concert_id)
        for i in range(1, capacity+1):
            if i in booked_seat_numbers:
                book_info = result[count]
                count = count + 1
                form_print(str(book_info['seat_number']), 30)
                form_print(str(book_info['audience_id']), 30, newLine=True)
            else:
                form_print(str(i), 30)
                form_print(" ", 30, newLine=True)
        line_print()
        blank_print()
        self.db.commit()

    # 16
    def reset_db(self):
        sql1 = 'drop table book;'
        sql2 = 'drop table assign;'
        sql3 = 'drop table hall;'
        sql4 = 'drop table concert;'
        sql5 = 'drop table audience;'
        sql6 = '''
            create table hall (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                location VARCHAR(200) NOT NULL,
                capacity INT NOT NULL
            );
        '''
        sql7 = '''
            create table concert (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                type VARCHAR(200) NOT NULL,
                price INT NOT NULL
            );
        '''
        sql8 = '''
            create table audience (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                gender CHAR(1) NOT NULL,
                age INT NOT NULL
            );
        '''
        sql9 = '''
            create table assign (
                id INT AUTO_INCREMENT PRIMARY KEY,
                hall_id INT NOT NULL,
                concert_id INT NOT NULL UNIQUE,
                CONSTRAINT FOREIGN KEY (hall_id) REFERENCES hall (id)
                ON DELETE CASCADE,
                CONSTRAINT FOREIGN KEY (concert_id) REFERENCES concert (id)
                ON DELETE CASCADE
            );
        '''
        sql10 = '''
            create table book (
                id INT AUTO_INCREMENT PRIMARY KEY,
                assign_id INT NOT NULL,
                audience_id INT NOT NULL,
                seat_number INT NOT NULL,
                CONSTRAINT FOREIGN KEY (assign_id) REFERENCES assign (id)
                ON DELETE CASCADE,
                CONSTRAINT FOREIGN KEY (audience_id) REFERENCES audience (id)
                ON DELETE CASCADE,
                CONSTRAINT UNIQUE (assign_id, seat_number)
            );
        '''
        self.cursor.execute(sql1)
        self.cursor.execute(sql2)
        self.cursor.execute(sql3)
        self.cursor.execute(sql4)
        self.cursor.execute(sql5)
        self.cursor.execute(sql6)
        self.cursor.execute(sql7)
        self.cursor.execute(sql8)
        self.cursor.execute(sql9)
        self.cursor.execute(sql10)
        self.db.commit()

init_message = '''============================================================
1. print all buildings
2. print all performances
3. print all audiences
4. insert a new building
5. remove a building
6. insert a new performance
7. remove a performance
8. insert a new audience
9. remove an audience
10. assign a performance to a building
11. book a performance
12. print all performances which assigned at a building
13. print all audiences who booked for a performance 14. print ticket booking status of a performance
15. exit
16. reset database
============================================================'''
print(init_message)

db = Data()

while(True):
    op = int(input('Select your action: '))
    if op == 1:
        db.show_all_hall()
    elif op == 2:
        db.show_all_concert()
    elif op == 3:
        db.show_all_audience()
    elif op == 4:
        name = input('Building name: ')
        location = input('Building location: ')
        capacity = int(input('Building capacity: '))
        if capacity < 1:
            print('Capacity should be more than 0\n')
            continue
        db.insert_hall(name, location, capacity)
        print('A building is successfully inserted\n')
    elif op == 5:
        id = int(input('Building ID: '))
        if not db.is_hall_exist(id):
            print('Building {} doesn’t exist\n'.format(id))
            continue
        db.delete_hall(id)
        print('A building is successfully removed\n')
    elif op == 6:
        name = input('Performance name: ')
        type = input('Performance type: ')
        price = int(input('Performance price: '))
        if price < 0:
            print('Price should be 0 or more\n')
            continue
        db.insert_concert(name, type, price)
        print('A performance is successfully inserted\n')
    elif op == 7:
        id = int(input('Performance ID: '))
        if not db.is_concert_exist(id):
            print('Performance {} doesn’t exist\n'.format(id))
            continue
        db.delete_concert(id)
        print('A performance is successfully removed\n')
    elif op == 8:
        name = input('Audience name: ')
        gender = input('Audience gender: ')
        if gender not in ('M', 'F'):
            print("Gender should be 'M' or 'F'\n")
            continue
        age = int(input('Audience age: '))
        if age < 1:
            print('Age should be more than 0\n')
            continue
        db.insert_audience(name, gender, age)
        print('An audience is successfully inserted\n')
    elif op == 9:
        id = int(input('Audience ID: '))
        if not db.is_audience_exist(id):
            print('Audience {} doesn’t exist\n'.format(id))
            continue
        db.delete_audience(id)
        print('An audience is successfully removed\n')
    elif op == 10:
        hall_id = int(input('Building ID: '))
        concert_id = int(input('Performance ID: '))
        assign_info = db.get_assign_info(concert_id)
        if len(assign_info) != 0:
            print('Performance {} is already assigned\n'.format(concert_id))
            continue
        db.assign(hall_id, concert_id)
        print('Successfully assign a performance\n')
    elif op == 11:
        concert_id = int(input('Performance ID: '))
        assign_info = db.get_assign_info(concert_id)
        if len(assign_info) == 0:
            print("Performance {} isn't assigned\n".format(concert_id))
            continue
        price = db.get_price(concert_id)
        assign_id = assign_info[0]
        hall_id = assign_info[1]
        capacity = db.get_capacity(hall_id)
        booked_seat_numbers = db.get_booked_seat_number(concert_id)
        audience_id = int(input('Audience ID: '))
        age = db.get_age(audience_id)
        seat_number_string = input('Seat number: ')
        seat_numbers = [int(seat) for seat in seat_number_string.split(',')]
        # check range error
        range_error = False
        for seat in seat_numbers:
            if seat not in range(1, capacity+1):
                range_error = True
        if range_error:
            print('Seat number out of range\n')
            continue
        # check taken error
        taken_error = False
        for seat in seat_numbers:
            if seat in booked_seat_numbers:
                taken_error = True
        if taken_error:
            print('The seat is already taken\n')
            continue
        # booking
        for seat in seat_numbers:
            db.book(assign_id, audience_id, seat)
        # adjust price according to age of audience
        # make price as float
        price = float(price)
        if age in range(1, 8):
            price = 0.0
        elif age in range(8, 13):
            price = price * 0.5
        elif age in range(13, 19):
            price = price * 0.8
        total_price = len(seat_numbers) * price
        total_price = round(total_price)
        print('Successfully book a performance')
        print('Total ticket price is {}\n'.format(total_price))
    elif op == 12:
        hall_id = int(input('Building ID: '))
        if not db.is_hall_exist(hall_id):
            print('Building {} doesn’t exist\n'.format(hall_id))
            continue
        db.show_assigned_concert(hall_id)
    elif op == 13:
        concert_id = int(input('Performance ID: '))
        if not db.is_concert_exist(concert_id):
            print('Performance {} doesn’t exist\n'.format(concert_id))
            continue
        db.show_booked_audience(concert_id)
    elif op == 14:
        concert_id = int(input('Performance ID: '))
        if not db.is_concert_exist(concert_id):
            print('Performance {} doesn’t exist\n'.format(concert_id))
            continue
        assign_info = db.get_assign_info(concert_id)
        if len(assign_info) == 0:
            print("Performance {} isn't assigned\n".format(concert_id))
            continue
        db.show_seat_state(concert_id)
    elif op == 15:
        print('Bye!')
        break
    elif op == 16:
        check = input('You are trying to reset whole database. Are you sure? Type [y/n]: ')
        if check == 'y':
            db.reset_db()
        blank_print()
    else:
        print('Invalid action\n')

del db