from pyspark import SparkContext, SparkFiles, SparkConf
from urllib.parse import unquote
import sys
import re
import datetime as dt

import io
import hashlib


def strStr(haystack: str, needle: str) -> int:
    def KMP(s, p):
        """
        s为主串
        p为模式串
        如果t里有p，返回打头下标
        """
        nex = getNext(p)
        i = 0
        j = 0   # 分别是s和p的指针
        while i < len(s) and j < len(p):
            if j == -1 or s[i] == p[j]: # j==-1是由于j=next[j]产生
                i += 1
                j += 1
            else:
                j = nex[j]

        if j == len(p): # j走到了末尾，说明匹配到了
            return i - j
        else:
            return -1

    def getNext(p):
        """
        p为模式串
        返回next数组，即部分匹配表
        """
        nex = [0] * (len(p) + 1)
        nex[0] = -1
        i = 0
        j = -1
        while i < len(p):
            if j == -1 or p[i] == p[j]:
                i += 1
                j += 1
                nex[i] = j     # 这是最大的不同：记录next[i]
            else:
                j = nex[j]

        return nex

    return KMP(haystack, needle)




def uncode(query, method='gb18030'):
    new_query = unquote(query, method)
    return new_query

def clean(query):
    L = ['\b', '\f', '\n', '\r', '\t', '\v']
    for c in L:
        query = query.replace(c, ' ')
    return query

def url_code(url):
    if url.endswith('/'):
        url = url[:-1]
    if url.startswith('http://'):
        url = url[7:]
    elif url.startswith('https://'):
        url = '@' + url[8:]
    return url




def extract_clicklog(line):
    line = line.strip('\n')
    if len(line) == 0:
        return []
    tmp = line.split('\t')
    if len(tmp) > 5 and (len(tmp)-2) % 4 == 0 and '#' in tmp[5]:
        if len(tmp[0].split('#')) == 6:
            '''【更旧】搜索展现日志'''
            '''缺少stype、chanel、source、isview、exposure、isjuhe、pagetype'''
            uid, uuid, page, time, unk1, unk2 = tmp[0].split('#')
            query = uncode(tmp[1])
            res = [query, [], []]
            for d in table.value:
                if strStr(query, d) != -1:
                    res[1].append(d)
            if len(res[1]) == 0:
                return [] 
            for i in range(2, len(tmp), 4):
                wapurl, isclick, clicktime, vwtwpit = tmp[i:i+4]
                vrid, web2wap, tc_flag, weburl, title = vwtwpit.split('#')
                res[2].append(vrid)
            return res 

        elif len(tmp[0].split('#')) == 9:
            '''【旧】搜索展现日志'''
            '''缺少isview、exposure、isjuhe'''
            uid, uuid, page, time, unk1, unk2, stype, chanel, source = tmp[0].split('#')
            query = uncode(tmp[1])
            res = [query, [], []]
            for d in table.value:
                if strStr(query, d) != -1:
                    res[1].append(d)
            if len(res[1]) == 0:
                return [] 
            for i in range(2, len(tmp), 4):
                wapurl, isclick, clicktime, vwtwpit = tmp[i:i+4]
                vrid, web2wap, tc_flag, weburl, pagetype, title = vwtwpit.split('#')
                res[2].append(vrid)
            return res
    elif len(tmp) > 7 and (len(tmp)-2) % 6 == 0 and '#' in tmp[7]:
        '''【新】带title的搜索展现日志'''
        uid, uuid, page, time, unk1, unk2, stype, chanel, source = tmp[0].split('#')
        query = uncode(tmp[1])
        res = [query, [], []]
        for d in table.value:
            if strStr(query, d) != -1:
                res[1].append(d)
        if len(res[1]) == 0:
            return [] 
        for i in range(2, len(tmp), 6):
            wapurl, isview, exposure, isclick, clicktime, vwtwpit = tmp[i:i+6]
            vrid, web2wap, tc_flag, weburl, pagetype, isjuhe, title = vwtwpit.split('#')
            res[2].append(vrid)
        return res
    return []
    
def mergeAndMove(line):
    key, value = line[0], line[1]
    n = len(value)
    keywords = set()
    for i in range(n):
        for w in value[i][0]:
            keywords.add(w)
    vrid = value[0][1]
    return [key, list(keywords), vrid, n]


    
if __name__ == "__main__":
    
    sc = SparkContext()
    
    # 读词表
    table_path = sys.argv[1]
    table = sc.textFile(table_path)
    table = table.collect()
    table = sc.broadcast(table)
    
    # 读一天的点击日志
    click_path = sys.argv[2]
    clickInput = sc.textFile(click_path, use_unicode=True)
    clickRdd = clickInput.map(extract_clicklog).filter(lambda x: x and len(x) != 0) # [query, [keywords], [vrids]]
    clickPair = clickRdd.map(lambda x: (x[0], x[1:])) # (query, [[keywords], [vrid]])
    clickPairGroupByKey = clickPair.groupByKey().mapValues(list) # (query, [([keywords], [vrids]), ([keywords], [vrids]), ([keywords], [vrids])])
    clickMergeAndMove = clickPairGroupByKey.map(mergeAndMove) # [query, keywords, vrids, n]
    clickJson = clickMergeAndMove.map(lambda x: {"query": x[0], "words": x[1], "vrids": x[2], "count": x[3]}).coalesce(1)


    # 保存
    save_path = sys.argv[3]
    clickJson.saveAsPickleFile(save_path)
    
    
    
  