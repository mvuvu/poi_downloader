import os
import pandas as pd

# 讀入餐廳清單 csv 檔，並以 dataframe 輸出
def read_csv2df(folder_name, file_name):
    # 讀入餐廳清單 csv 檔
    path = os.getcwd() + folder_name
    restaurant_list_df = pd.read_csv(os.path.join(path , file_name + '.csv'))
    
    return restaurant_list_df


# 將餐廳評論存入指定的資料中
def save2csv(folder_name, dataframe, file_name):
    path = os.getcwd() + '/result/{}'.format(folder_name)
    
    os.chdir(path)
    dataframe.to_csv(file_name + '.csv', index=False)
    
    os.chdir('../..')


# drop 掉空評論
def drop_no_comment(dataframe):
    no_comment_index = dataframe[(dataframe['comment_text'] == '') == True].index
    clean_df = dataframe.drop(no_comment_index)
    # print('共有 {} 則有效評論'.format(len(clean_df)))
    print()

    return clean_df


# 取得已經存在於餐廳評論資料夾的檔案
def get_file_already_exist(folder_name):
    file_list = os.listdir('result/{}'.format(folder_name))
    exist_file_id_list = []
    
    for file in file_list:
        # mac 會生成此一檔案
        if file == '.DS_Store':
            continue

        exist_file_id_list.append(file.split('-----')[1].replace('.csv', ''))
    
    return exist_file_id_list


def whether_file_exist(folder_name):
    # 如果尚未有同名的資料夾，便創建資料夾
    if os.path.exists('result/{}'.format(folder_name)) == False:
        os.chdir(os.getcwd() + '/result')
        os.mkdir(folder_name)
        os.chdir('..')
        exist_file_id_list = []
    
    # 如果已有同名的資料夾，便取得其中已有的檔案清單
    elif os.path.exists('result/{}'.format(folder_name)) == True:
        exist_file_id_list = get_file_already_exist(folder_name)

    return exist_file_id_list

