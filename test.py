

def func(flag = 1):
    if flag == 1:
        print("flag = 1")
    else:
        print("flag = 0")

if __name__ == "__main__":
    flag= 1
    dic = {
        "st1": 1
        ,"flag": 1
    }
    func(dic.get('flag'))