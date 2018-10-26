
user,passwd='andy','123abc'
def auth(auth_type):
    def out_wrapper(func):
        def wrapper(*args,**kwargs):
            if auth_type == 'local':
                username=input("Input username:")
                password=input("Input password:")
                if user==username and passwd == password:
                    print("Welcome!")
                    return func()
                else:
                    print("Wrong username or password!")
                    return wrapper(*args, **kwargs)
            elif auth_type == 'ldap':
                print("Sorry, ldap isn't work!")
        return wrapper
    return out_wrapper
def index():
    print("welcome to index page")
@auth(auth_type="local")
def home():
    print("welcome to home page")
@auth(auth_type="ldap")
def bbs():
    print("welcome to bbs page")


def outer(func):
    def inner(*args,**kwargs):
        print("认证成功！")
        result = func(*args,**kwargs)
        print("日志添加成功")
        return result
    return inner

@outer
def f1(name,age):
    print("%s 正在连接业务部门1数据接口 %s......"%(name,age))

# 调用方法
f1("jack",'18')
