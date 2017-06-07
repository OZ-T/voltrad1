import pandas as pd
import json
import datetime as dt

def CustomParser(data):
    # eval convert unicode string to dictionary
    j1 = eval(data)
    return j1

def partition_data_set_by_option_expiry():
    """
         
    """
    hdf5_path = "C:/Users/David/data/optchain_yahoo_hist_db.h5"
    hdf5_out_template = "C:/Users/David/data/optchain_yahoo_db_expiry_"

    store_file = pd.HDFStore(hdf5_path,mode='w')
    root1 = store_file.root
    list = [x._v_pathname for x in root1]
    store_file.close()
    print (list)
    for node in list:
        store_file = pd.HDFStore(hdf5_path,mode='w')
        node1 = store_file.get_node(node)
        if node1:
            df1 = store_file.select(node1._v_pathname)
            # Unfortunately th JSON field doesnt contain more info that the already present fields
            # df1['json_dict'] = df1['JSON'].apply(CustomParser)
            # following line converts dict column into n columns for the dataframe:
            # https://stackoverflow.com/questions/20680272/reading-a-csv-into-pandas-where-one-column-is-a-json-string
            # df1 = pd.concat([df1.drop(['json_dict','JSON'], axis=1), df1['json_dict'].apply(pd.Series)], axis=1)
            df1 = df1.drop(['JSON'], axis=1)
            # Create a dictionary that holds data sets for each expiry
            # sort the dataframe
            df1.sort(columns=['Expiry'], inplace=True)
            # set the index to be this and don't drop
            df1.set_index(keys=['Expiry'], drop=False, inplace=True)
            # get a list of names
            # names = df1['Expiry'].unique().tolist()
            names = df1['Expiry'].dt.strftime("%Y-%m").unique().tolist()
            for x in names:
                out_store_file = pd.HDFStore(hdf5_out_template + x + ".h5",mode='w')
                # now we can perform a lookup on a 'view' of the dataframe
                dataframe = df1.loc[df1['Expiry'].dt.strftime("%Y-%m") == x]
                print(("Node: ", node1._v_pathname, "Total rows in input ds: ", len(df1), "No. Rows for expiry: ", len(dataframe), "Expiry Month: ", x))
                # Following code is to FIX ERROR: TypeError: [unicode] is not implemented as a table column START
                types = dataframe.apply(lambda x: pd.lib.infer_dtype(x.values))
                for col in types[types == 'unicode'].index:
                    dataframe[col] = dataframe[col].astype(str)
                dataframe.columns = [str(c) for c in dataframe.columns]
                # END
                out_store_file.append(node, dataframe, data_columns=True)
                out_store_file.close()
        store_file.close()


if __name__ == "__main__":
    partition_data_set_by_option_expiry()