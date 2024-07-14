# uname() error回避
import platform
print("platform", platform.uname())

from sqlalchemy import create_engine, insert, delete, select, text
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
import pandas as pd

from db_control.connect import engine
from db_control.mymodels import Customers

def myinsert(mymodel, values):
    # IDのバリデーション
    if not values.get("customer_id"):
        return {"error": "customer_id is required"}, 400
    
    # session構築
    Session = sessionmaker(bind=engine)
    session = Session()

    query = insert(mymodel).values(values)
    try:
        # トランザクションを開始
        with session.begin():
            # データの挿入
            result = session.execute(query)
    except sqlalchemy.exc.IntegrityError as e:
        print(f"一意制約違反により、挿入に失敗しました: {e}")
        session.rollback()

    # セッションを閉じる
    session.close()
    return "inserted"

def myselect(mymodel, customer_id):
    # session構築
    Session = sessionmaker(bind=engine)
    session = Session()
    query = session.query(mymodel).filter(mymodel.customer_id == customer_id)
    try:
        # トランザクションを開始
        with session.begin():
            result = query.all()
        # 結果をオブジェクトから辞書に変換し、リストに追加
        result_dict_list = []
        for customer_info in result:
            result_dict_list.append({
                "customer_id": customer_info.customer_id,
                "customer_name": customer_info.customer_name,
                "age": customer_info.age,
                "gender": customer_info.gender
            })
        # リストをJSONに変換
        result_json = json.dumps(result_dict_list, ensure_ascii=False)
    except sqlalchemy.exc.IntegrityError as e:
        print(f"一意制約違反により、挿入に失敗しました: {e}")

    # セッションを閉じる
    session.close()
    return result_json

def myselectAll(mymodel):
    # session構築
    Session = sessionmaker(bind=engine)
    session = Session()
    query = select(mymodel)
    try:
        # トランザクションを開始
        with session.begin():
            df = pd.read_sql_query(query, con=engine)
            result_json = df.to_json(orient='records', force_ascii=False)
    except sqlalchemy.exc.IntegrityError as e:
        print(f"一意制約違反により、挿入に失敗しました: {e}")
        result_json = None

    # セッションを閉じる
    session.close()
    return result_json

def myupdate(mymodel, values):
    # session構築
    Session = sessionmaker(bind=engine)
    session = Session()

    customer_id = values.pop("customer_id")

    # パラメータをバインドするクエリ
    query = text("""
    UPDATE customers 
    SET customer_name = :customer_name, age = :age, gender = :gender 
    WHERE customer_id = :customer_id
    """)

    try:
        # トランザクションを開始
        with session.begin():
            result = session.execute(query, {
                'customer_name': values['customer_name'],
                'age': values['age'],
                'gender': values['gender'],
                'customer_id': customer_id
            })
    except sqlalchemy.exc.IntegrityError as e:
        print(f"一意制約違反により、更新に失敗しました: {e}")
        session.rollback()
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
        session.rollback()
    # セッションを閉じる
    session.close()
    return "put"

def mydelete(mymodel, customer_id):
    # session構築
    Session = sessionmaker(bind=engine)
    session = Session()
    query = delete(mymodel).where(mymodel.customer_id == customer_id)
    try:
        # トランザクションを開始
        with session.begin():
            result = session.execute(query)
    except sqlalchemy.exc.IntegrityError as e:
        print(f"一意制約違反により、削除に失敗しました: {e}")
        session.rollback()

    # セッションを閉じる
    session.close()
    return customer_id + " is deleted"
