import requests
from bs4 import BeautifulSoup


def extract(url):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    # 找到表格
    table = data.find("table", {"class": "wikitable sortable mw-collapsible"})
    if not table:
        print("未找到表格")
        return

    # 获取 <tbody>
    table_body = table.find('tbody')
    if not table_body:
        print("未找到 <tbody>")
        return

    # 获取所有 <tr>
    rows = table_body.find_all('tr', recursive=False)  # 避免嵌套解析
    for row in rows:
        # 过滤掉可能的 <th> 头部行
        if row.find('th'):
            continue
        print(row)


# 你可以替换 URL 进行测试
extract("https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks")

