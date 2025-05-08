def filter_and_save(file_a, file_b, file_c):
    with open(file_a, 'r', encoding='utf-8') as fa, \
            open(file_b, 'r', encoding='utf-8') as fb, \
            open(file_c, 'w', encoding='utf-8') as fc:

        for line_a in fa:
            line_a = line_a.strip()  # 去除行尾的换行符和空格
            fb.seek(0)  # 每次循环开始时重置file_b的读取位置

            for line_b in fb:
                line_b = line_b.strip()  # 去除行尾的换行符和空格
                if line_b in line_a:
                    fc.write(line_a + '\n')  # 将符合条件的b写入C文本


# 使用示例
file_a = 'C:/Users/22565/Downloads/downloadList.txt'
file_b = 'C:/Users/22565/Desktop/noharmList.txt'
file_c = 'C:/Users/22565/Desktop/save.txt'

filter_and_save(file_a, file_b, file_c)