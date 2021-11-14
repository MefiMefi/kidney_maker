import logging

import numpy as np
import pandas as pd


# import eel
# eel.init('web')
# eel.start('index.html')


def main():
    excel_reader = pd.ExcelFile("tables/诚2团妈位表(2).xlsx")
    sheet_names = excel_reader.sheet_names
    df_sort = excel_reader.parse(sheet_name=sheet_names[0], header=None)
    df_sort.dropna(axis=1,how="all",inplace = True)
    df_sort.dropna(axis=0, how="all", inplace=True)
    df_price = excel_reader.parse(sheet_name=sheet_names[1], header=None)

    # print(df_sort)


    # custom process

    # df_hold = excel_reader.parse(sheet_name=sheet_names[2], header=None)

    detail_df, sort_df, orders_df = sort_table(df_sort)
    price_dict = adjust_price(df_price)

    xlsx_writer = pd.ExcelWriter("诚2团妈位表_bill.xlsx")
    df_bill = calc_total_price(sort_df, price_dict)
    # df_bill.to_excel(xlsx_writer, sheet_name="肾表")

    # hold_dict = adjust_price(df_hold)
    # df_hold_bill = calc_total_price(sort_df, hold_dict)
    # df_hold_bill = df_hold_bill[["CN", "总肾"]]
    # df_hold_bill.columns = ["CN", "定金"]
    # df_price_hold = pd.merge(df_bill, df_hold_bill)
    # df_price_hold["预期补款"] = df_price_hold["总肾"] - df_price_hold["定金"]
    # df_price_hold.to_excel(xlsx_writer, sheet_name="肾表", )
    df_bill.to_excel(xlsx_writer, sheet_name="肾表", )
    orders_df.to_excel(xlsx_writer,sheet_name="下单表")

    xlsx_writer.save()
    # df_formatted = pd.DataFrame({"CN": []})
    # for idx, sheet_name in enumerate(sheet_names):
    #     df = excel_reader.parse(sheet_name=sheet_names[idx], header=None)
    #     df_out = sort_table(df, sheet_name)
    #     df_formatted = pd.merge(df_formatted, df_out, on="CN", how="outer")
    # df_formatted["吧唧总肾"] = df_formatted["吧唧总计"] * 25.2
    # df_formatted["相卡总肾"] = df_formatted["相卡总计"] * 15.75
    # df_formatted["吧唧总肾"].fillna(0, inplace=True)
    # df_formatted["相卡总肾"].fillna(0, inplace=True)
    # df_formatted["总肾"] = df_formatted["吧唧总肾"] + df_formatted["相卡总肾"]
    # df_formatted.to_excel("SS4_肾表明细.xlsx")


def goods_count(df):
    counts = df.iloc[0:, 1:].fillna(0).applymap(lambda x: 1 if x != 0 else 0)
    idol_col = df.iloc[0:,0:1]
    counts["orders"] = counts.apply(lambda x: sum(x),axis = 1)
    counts["idol"] = idol_col

    return counts[["idol","orders"]]


def sort_table(df):
    df.rename(columns={0: "idol"}, inplace=True)
    orders_count = goods_count(df)
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

    # df_out["price"] = df_out["count"] * price
    # df_out.columns = ["CN", df_name + "明细", df_name + "总计"]
    return df_out, df_count,orders_count


def adjust_price(df):
    df.columns = ["idol", "avg", "adj"]
    df["new"] = df["avg"] + df["adj"]
    dict_ = df[["idol", "new"]].to_dict(orient="records")
    dict_ = {x["idol"]: x["new"] for x in dict_}
    print(dict_)
    return dict_


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
    df_out.columns = ["CN", "明细", "总数", "总肾"]
    return df_out


if __name__ == "__main__":
    main()
