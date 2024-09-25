def similiar_movies():
    import pandas as pd
    from pipeline2_df import read_data_in_df
    user_df,rating_df,item_data=read_data_in_df()

    from scipy.stats import pearsonr
    import numpy as np

    # 1. Create a User-Movie Ratings Matrix
    # Pivot rating_df to create user-movie matrix
    user_movie_matrix = rating_df.pivot(index='user_id', columns='item_id', values='rating')

    # 2. Calculate Similarity Between Movies
    # Create a movie-to-movie similarity dictionary
    def calculate_similarity(movie_id, movie_ratings_matrix, threshold=50):
        similar_movies = []
        target_movie_ratings = movie_ratings_matrix[movie_id]
        
        for other_movie_id in movie_ratings_matrix.columns:
            if other_movie_id == movie_id:
                continue
                
            # Get common users who rated both movies
            common_users = target_movie_ratings.dropna().index.intersection(
                movie_ratings_matrix[other_movie_id].dropna().index)
            
            # Only consider movies with enough co-occurrence
            if len(common_users) >= 50:
                # Calculate Pearson correlation for similarity
                similarity, _ = pearsonr(
                    movie_ratings_matrix[movie_id].loc[common_users],
                    movie_ratings_matrix[other_movie_id].loc[common_users]
                )
                
                # If similarity is greater than or equal to the similarity threshold (0.95)
                if similarity >= 0.0:
                    similar_movies.append((other_movie_id, similarity, len(common_users)))
        
        return similar_movies

    # 3. Find top 10 similar movies for a given movie
    def find_top_similar_movies(movie_id, movie_ratings_matrix):
        similar_movies = calculate_similarity(movie_id, movie_ratings_matrix)
        
        # Sort movies by similarity score and co-occurrence count
        similar_movies_sorted = sorted(similar_movies, key=lambda x: (-x[1], -x[2]))[:10]
        
        return similar_movies_sorted

    # Example: Finding top 10 similar movies to a given movie (say movie_id = 1)
    movie_id = 1  # Replace with the ID of the movie you're interested in
    top_similar_movies = find_top_similar_movies(movie_id, user_movie_matrix)

    # Display the results (movie_id, similarity score, co-occurrence count)
    print("Top 10 similar movies:")
    for movie, similarity, co_occurrence in top_similar_movies:
        print(f"Movie ID: {movie}, Similarity: {similarity:.4f}, Co-occurrence: {co_occurrence}")

#similiar_movies()