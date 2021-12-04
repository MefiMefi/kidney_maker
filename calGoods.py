import logging
import eel
import pandas as pd


@eel.expose
def kidney_maker(source_path, target_dir):
    df_sort, df_price, df_origin = xlsx_read_and_preprocess(source_path)
    detail_df, sort_df = sort_table(df_sort)
    price_df = adjust_price(df_price)
    origin_df = None if df_origin is None else get_original_bill(df_origin)
    bill_df = calc_total_price(sort_df, price_df, detail_df, origin_df)
    bill_df = format_kidney_table(bill_df)
    with pd.ExcelWriter(target_dir) as xlsx_writer:
        bill_df.to_excel(xlsx_writer, sheet_name="肾表", index=None)


def xlsx_read_and_preprocess(path):
    excel_reader = pd.ExcelFile(path)
    sheet_names = excel_reader.sheet_names
    try:
        df_sort = excel_reader.parse(sheet_name=sheet_names[0], header=None)
    except:
        print("排表传入错误")

    try:
        df_price = excel_reader.parse(sheet_name=sheet_names[1], header=None)
    except:
        print("调价表传入错误")

    try:
        df_origin = excel_reader.parse(sheet_name=sheet_names[2], header=None)
    except:
        df_origin = None

    excel_reader.close()

    return df_sort, df_price, df_origin


def make_details(type, idol, count):
    type = str(type)
    idol = str(idol)
    count = str(count)
    if type == "":
        details = "{}:{}".format(idol, count)
    else:
        details = "{}-{}:{}".format(type, idol, count)
    return details


def sort_table(df):
    df.rename(columns={0: "type", 1: "idol"}, inplace=True)
    df["type"].fillna("", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)
    df.dropna(axis=0, how="all", inplace=True)

    df_long = pd.DataFrame()
    for i in range(2, (len(df.columns))):
        df.rename(columns={i: "buyer_{}".format(i)}, inplace=True)
        tmp_df = df[["type", "idol", "buyer_{}".format(i)]]
        tmp_df.columns = ["type", "idol", "buyer"]
        df_long = pd.concat([df_long, tmp_df], ignore_index=True)
    df_long["count"] = 1
    df_count = df_long.groupby(["type", "idol", "buyer"], as_index=False).sum()
    df_detail = df_count.copy()
    df_detail["detail"] = df_detail.apply(lambda x: make_details(x["type"], x["idol"], x["count"])
                                          , axis=1)
    df_detail = df_detail[["buyer", "type", "detail"]]
    df_detail = df_detail.groupby('buyer')['detail'].apply(lambda x: x.str.cat(sep=' ,')).reset_index()
    df_sum = df_count[["buyer", "count"]].groupby("buyer", as_index=False).sum()
    df_out = pd.merge(df_detail, df_sum)

    return df_out, df_count


def sort_table_pld(df):
    df.rename(columns={0: "version", 1: "type", 2: "idol"}, inplace=True)
    df_long = pd.DataFrame()
    df["type"].fillna("-", inplace=True)
    for i in range(3, (len(df.columns))):
        df.rename(columns={i: "buyer_{}".format(i)}, inplace=True)
        tmp_df = df[["version", "type", "idol", "buyer_{}".format(i)]]
        tmp_df.columns = ["version", "type", "idol", "buyer"]
        df_long = pd.concat([df_long, tmp_df], ignore_index=True)
    df_long["count"] = 1
    print(df_long)
    df_count = df_long.groupby(["buyer", "version", "type", "idol"], as_index=False).sum()
    df_detail = df_count.copy()
    df_detail["detail"] = df_detail.apply(
        lambda x: "{}-{}-{}:{}".format(x["version"], x["type"], x["idol"], x["count"]), axis=1)
    df_detail = df_detail[["buyer", "version", "type", "detail"]]

    df_detail = df_detail.groupby('buyer', )['detail'].apply(lambda x: x.str.cat(sep=',')).reset_index()
    df_sum = df_count[["buyer", "count"]].groupby("buyer", as_index=False).sum()
    df_out = pd.merge(df_detail, df_sum)

    # df_out["price"] = df_out["count"] * price
    # df_out.columns = ["CN", df_name + "明细", df_name + "总计"]
    print(df_count[df_count["version"] == "追忆"])
    return df_out, df_count


def adjust_price(df):
    if len(df.columns) < 4:
        df["adj"] = 0
    df.columns = ["type", "idol", "avg", "adj"]
    df["adj"].fillna(0, inplace=True)
    df["avg"].fillna(0, inplace=True)
    df["type"].fillna("", inplace=True)
    df["new"] = df["avg"] + df["adj"]

    return df[["type", "idol", "new"]]


def adjust_price_pld(df, special_key="签", normal_keys=["花前", "花后"], unique_key="追忆"):
    if len(df.columns) < 5:
        df["adj"] = 0
    df.columns = ["version", "type", "idol", "avg", "adj"]
    df["adj"].fillna(0, inplace=True)
    df["type"].fillna("-", inplace=True)
    df["new"] = df["avg"] + df["adj"]
    price_dict_df = df[["version", "type", "idol", "new"]]
    price_dict_df["special"] = price_dict_df["type"].map(lambda x: 1 if special_key in x else 0)
    price_dict_df["unique"] = price_dict_df["version"].map(lambda x: 1 if x == unique_key else 0)
    normal_dict = price_dict_df[price_dict_df["special"] == 0]
    special_dict = price_dict_df[price_dict_df["special"] == 1]
    unique_dict = normal_dict[normal_dict["unique"] == 1]
    normal_dict = normal_dict[normal_dict["unique"] == 0]
    normal_dict_copy = normal_dict.copy()
    for key_ in normal_keys:
        normal_dict_tmp = normal_dict_copy.copy()
        normal_dict_tmp["type"] = key_
        normal_dict = pd.concat([normal_dict, normal_dict_tmp], ignore_index=True)
    normal_dict = normal_dict[normal_dict["type"] != "-"]
    price_dict_df = pd.concat([normal_dict, special_dict, unique_dict], ignore_index=True)

    # dict_ = df[["version","type","idol", "new"]].to_dict(orient="records")
    # dict_ = {x["idol"]: x["new"] for x in dict_}
    price_dict_df.to_excel("tmp_price.xlsx")
    return price_dict_df


def get_original_bill(df):
    df.rename(columns={0: "buyer", 1: "original_bill"}, inplace=True)
    # print(df)
    return df


def calc_total_price(sort_df, price_df, detail_df, origin_df):
    sort_df = pd.merge(sort_df, price_df, how="outer", on=["type", "idol"])

    sort_df["bill"] = sort_df.apply(lambda x: x["count"] * x["new"], axis=1)
    sort_df["count"].fillna(0, inplace=True)
    # bill_df = sort_df[["buyer","bill"]]
    # # df_detail = sort_df.copy()
    # # df_detail["detail"] = df_detail.apply(lambda x: "{}:{}".format(x["idol"], x["count"]), axis=1)
    # # df_detail = df_detail[["buyer", "detail"]]
    # # df_detail = df_detail.groupby('buyer')['detail'].apply(lambda x: x.str.cat(sep=',')).reset_index()
    # # df_sum = sort_df[["buyer", "count"]].groupby("buyer", as_index=False).sum()
    bill_df = sort_df[["buyer", "bill"]].groupby("buyer", as_index=False).sum()
    bill_df = pd.merge(detail_df, bill_df, how="outer")

    bill_df["count"].fillna(0, inplace=True)
    bill_df["bill"].fillna(0, inplace=True)
    if origin_df is not None:
        bill_df = pd.merge(bill_df, origin_df, how="outer")
        bill_df["count"].fillna(0, inplace=True)
        bill_df["bill"].fillna(0, inplace=True)
        bill_df["original_bill"].fillna(0, inplace=True)
        bill_df["delta_bill"] = bill_df["bill"] - bill_df["original_bill"]
        bill_df["mark"] = bill_df["delta_bill"].map(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    # df_out = pd.merge(df_out, df_bill)
    # print(bill_df)
    return bill_df


def goods_count(df):
    counts = df.iloc[0:, 1:].fillna(0).applymap(lambda x: 1 if x != 0 else 0)
    idol_col = df.iloc[0:, 0:1]
    counts["orders"] = counts.apply(lambda x: sum(x), axis=1)
    counts["idol"] = idol_col

    return counts[["idol", "orders"]]


def format_kidney_table(df):
    mark_dict = {
        1: "补",
        0: "",
        -1: "退"
    }
    try:
        df["mark"] = df["mark"].map(lambda x: mark_dict.get(x))
    except:
        pass
    df.rename(columns={"buyer": "CN",
                       "detail": "明细",
                       "count": "计数",
                       "bill": "总肾",
                       "original_bill": "原肾",
                       "delta_bill": "退补金额",
                       "mark": "退补标记"}, inplace=True)
    df["CN_1"] = df["CN"]
    return df


def calc_total_price_pld(sort_df, price_dict):
    sort_df = pd.merge(sort_df, price_dict, how="outer", on=["version", "type", "idol"])
    sort_df["bill"] = sort_df.apply(lambda x: x["count"] * x["new"], axis=1)
    sort_df["count"].fillna(0, inplace=True)
    sort_df = sort_df[["buyer", "version", "type", "idol", "count", "new", "bill"]]
    # print(sort_df)

    df_detail = sort_df.copy()
    df_detail["detail"] = df_detail.apply(
        lambda x: "{}-{}-{}:{}".format(x["version"], x["type"], x["idol"], str(int(x["count"]))), axis=1)
    df_detail = df_detail[["buyer", "detail"]]
    df_detail = df_detail.groupby('buyer')['detail'].apply(lambda x: x.str.cat(sep=',')).reset_index()
    df_sum = sort_df[["buyer", "count"]].groupby("buyer", as_index=False).sum()
    df_bill = sort_df[["buyer", "bill"]].groupby("buyer", as_index=False).sum()
    df_out = pd.merge(df_detail, df_sum)
    df_out = pd.merge(df_out, df_bill)
    df_out["count"] = df_out["count"].map(lambda x: int(x))
    return df_out


if __name__ == "__main__":
    path = "模板表格.xlsx"
    target_name = path.split(".")[0]
    # target_dir = "./{}_bill.xlsx".format(target_name)
    target_dir = "./模板表格_bill.xlsx"
    kidney_maker(path, target_dir)
