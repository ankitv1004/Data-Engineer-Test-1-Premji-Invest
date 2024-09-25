def read_data_in_df():
    import pandas as pd
    user_df = pd.DataFrame(columns=['user_id', 'age', 'gender', 'occupation', 'zip'])
    file = open("dags/ml-100k/u.user", "r")
    for each_row in file:
        user_df = pd.concat([user_df, pd.DataFrame([each_row.split('|')],\
                                                    columns=['user_id', 'age', 'gender', 'occupation', 'zip'])],\
                                ignore_index=True)
    user_df['zip'] = [each_zip.rstrip("\n") for each_zip in user_df['zip']]
    user_df['age'] = [int(age) for age in user_df['age']]

    # Reading ratings data
    rating_df= pd.read_csv("dags/ml-100k/u.data", delimiter="\t", names=['user_id', 'item_id', 'rating', 'timestamp'])

    # Reading item data
    item_columns = ['movie_id', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action',
                        'Adventure', 'Animation', "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy',
                        'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    item_data = pd.DataFrame(columns=item_columns)
    genres = ['unknown', 'Action', 'Adventure', 'Animation', "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama',
                'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War',
                'Western']
    item_file = open("dags/ml-100k/u.item", "r", encoding='iso-8859-1')
    for x in item_file:
        item_data = pd.concat([item_data, pd.DataFrame([x.split('|')], columns=item_columns)], ignore_index=True)
    item_data['Western'] = [x.rstrip("\n") for x in item_data['Western']]
    item_data['movie_id'] = [int(x) for x in item_data['movie_id']]
    for genre in genres:
        item_data[genre] = [int(x) for x in item_data[genre]]
    
    return user_df,rating_df,item_data
#print(read_data_in_df())
