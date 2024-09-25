def top_genres():
    import pandas as pd
    from pipeline2_df import read_data_in_df

    user_df,rating_df,item_data=read_data_in_df()

    user_df['user_id'] = user_df['user_id'].astype(int)
    rating_df['user_id'] = rating_df['user_id'].astype(int)
    rating_df['item_id'] = rating_df['item_id'].astype(int)
    item_data['movie_id'] = item_data['movie_id'].astype(int)

    # 1. Define age groups in user_df
    age_bins = [20, 25, 35, 45, 100] 
    age_labels = ['20-25', '25-35', '35-45', '45+']
    user_df['age_group'] = pd.cut(user_df['age'], bins=age_bins, labels=age_labels, right=False)

    # 2. Merge user_df, rating_df, and item_data on 'user_id' and 'item_id' (movie_id)
    merged_df = pd.merge(rating_df, user_df, on='user_id')
    merged_df = pd.merge(merged_df, item_data, left_on='item_id', right_on='movie_id')

    # 3. Group by occupation, age_group, and genres
    # List of genre columns in item_data
    genres = ['unknown', 'Action', 'Adventure', 'Animation', "Children's", 'Comedy', 'Crime', 
            'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 
            'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']

    # Melt the dataframe to count ratings for each genre
    genre_ratings = merged_df.melt(id_vars=['occupation', 'age_group', 'rating'], 
                                value_vars=genres, var_name='genre', value_name='is_genre')

    # Filter rows where 'is_genre' is 1 (indicating the movie belongs to that genre)
    genre_ratings = genre_ratings[genre_ratings['is_genre'] == 1]

    # 4. Aggregate ratings by occupation, age group, and genre, and sum the ratings
    genre_rating_summary = genre_ratings.groupby(['occupation', 'age_group', 'genre']).agg(
        total_ratings=('rating', 'sum')
    ).reset_index()

    # 5. For each occupation and age group, find the genre with the highest total rating
    top_genres_by_occupation_age = genre_rating_summary.loc[
        genre_rating_summary.groupby(['occupation', 'age_group'])['total_ratings'].idxmax()
    ]

    # Display the top genres for each occupation and age group
    print(top_genres_by_occupation_age[['occupation', 'age_group', 'genre', 'total_ratings']])

#top_genres()