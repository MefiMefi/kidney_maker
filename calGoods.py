import logging

import pandas as pd


def main():
    excel_reader = pd.ExcelFile("tables/诚2团妈位表.xlsx")
    sheet_names = excel_reader.sheet_names
    df_sort = excel_reader.parse(sheet_name=sheet_names[0], header=None)
    df_price = excel_reader.parse(sheet_name=sheet_names[1], header=None)
    df_origin = excel_reader.parse(sheet_name=sheet_names[2], header=None)
    df_origin.columns = ["buyer", "original_bill"]

    detail_df, sort_df = sort_table_muti(df_sort)
    price_dict = adjust_price_muti(df_price)

    df_bill = calc_total_price_muti(sort_df, price_dict)
    df_all = pd.merge(df_bill, df_origin, how="outer", on="buyer")
    df_all["bill"].fillna(0, inplace=True)
    df_all["original_bill"].fillna(0, inplace=True)
    df_all["additional_bill"] = df_all["bill"] - df_all["original_bill"]
    df_all["label"] = df_all["additional_bill"].map(lambda x: "退" if x < 0 else ("补" if x > 0 else ""))
    df_out = df_all[['buyer', 'detail', 'count', 'bill', 'original_bill', 'additional_bill',
                     'label']]
    df_out.columns = ["CN", "明细", "计数", "总价", "原肾", "退补金额", "退补"]
    df_out.to_excel("诚2团_bill.xlsx")


def sort_table(df):
    df.rename(columns={0: "idol"}, inplace=True)
    df_long = pd.DataFrame()
    for i in range(1, (len(df.columns))):
        df.rename(columns={i: "buyer_{}".format(i)}, inplace=True)
        tmp_df = df[["idol", "buyer_{}".format(i)]]
        tmp_df.columns = ["idol", "buyer"]
        df_long = pd.concat([df_long, tmp_df], ignore_index=True)
    df_long["count"] = 1
    df_count = df_long.groupby(["buyer", "idol"], as_index=False).sum()
    df_detail = df_count.copy()
    df_detail["detail"] = df_detail.apply(lambda x: "{}:{}".format(x["idol"], x["count"]), axis=1)
    df_detail = df_detail[["buyer", "detail"]]
    df_detail = df_detail.groupby('buyer')['detail'].apply(lambda x: x.str.cat(sep=',')).reset_index()
    df_sum = df_count[["buyer", "count"]].groupby("buyer", as_index=False).sum()
    df_out = pd.merge(df_detail, df_sum)
    print(df_out)
    # df_out["price"] = df_out["count"] * price
    # df_out.columns = ["CN", df_name + "明细", df_name + "总计"]
    return df_out, df_count


def sort_table_muti(df):
    df.rename(columns={0: "version", 1: "type", 2: "idol"}, inplace=True)
    df_long = pd.DataFrame()
    df["type"].fillna("-",inplace=True)
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
    print(df_count[df_count["version"]=="追忆"])
    return df_out, df_count


def adjust_price(df):
    df.columns = ["idol", "avg", "adj"]
    df["new"] = df["avg"] + df["adj"]
    dict_ = df[["idol", "new"]].to_dict(orient="records")
    dict_ = {x["idol"]: x["new"] for x in dict_}
    print(dict_)
    return dict_


def adjust_price_muti(df, special_key="签", normal_keys=["花前", "花后"], unique_key="追忆"):
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


def calc_total_price(sort_df, price_dict):
    sort_df["bill"] = sort_df.apply(lambda x: x["count"] * price_dict[x["idol"]], axis=1)
    print(sort_df)

    df_detail = sort_df.copy()
    df_detail["detail"] = df_detail.apply(lambda x: "{}:{}".format(x["idol"], x["count"]), axis=1)
    df_detail = df_detail[["buyer", "detail"]]
    df_detail = df_detail.groupby('buyer')['detail'].apply(lambda x: x.str.cat(sep=',')).reset_index()
    df_sum = sort_df[["buyer", "count"]].groupby("buyer", as_index=False).sum()
    df_bill = sort_df[["buyer", "bill"]].groupby("buyer", as_index=False).sum()
    df_out = pd.merge(df_detail, df_sum)
    df_out = pd.merge(df_out, df_bill)
    return df_out


def calc_total_price_muti(sort_df, price_dict):
    sort_df = pd.merge(sort_df, price_dict, how="outer", on=["version", "type", "idol"])
    sort_df["bill"] = sort_df.apply(lambda x: x["count"] * x["new"], axis=1)
    sort_df["count"].fillna(0, inplace=True)
    sort_df = sort_df[["buyer", "version", "type", "idol", "count", "new", "bill"]]
    print(sort_df)

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
    main()
