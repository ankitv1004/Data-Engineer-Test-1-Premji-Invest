def top_20_movies():
    import pandas as pd
    from pipeline2_df import read_data_in_df

    user_df,rating_df,item_data=read_data_in_df()

    # Merge rating_df with item_data to get movie titles
    merged_df = pd.merge(rating_df, item_data, left_on='item_id', right_on='movie_id')

    # Group by movie and calculate count of ratings and mean rating
    movie_ratings = merged_df.groupby(['movie_id', 'movie_title']).agg(
        rating_count=('rating', 'count'),
        avg_rating=('rating', 'mean')
    ).reset_index()

    # Filter movies that have been rated at least 35 times
    filtered_movies = movie_ratings[movie_ratings['rating_count'] >= 35]

    # Sort by average rating in descending order and select top 20
    top_20_movies = filtered_movies.sort_values(by='avg_rating', ascending=False).head(20)

    # Display the top 20 movies
    print(top_20_movies[['movie_title', 'avg_rating', 'rating_count']])

#top_20_movies()
