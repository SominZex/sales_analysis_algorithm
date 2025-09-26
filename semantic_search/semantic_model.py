import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import DBSCAN
import re
import pickle
from typing import List, Dict, Tuple
import warnings
warnings.filterwarnings('ignore')

class ProductSimilarityModel:
    """
    A model to find semantic similarity between products and group similar products together.
    This helps in analyzing product families rather than individual SKUs.
    """
    
    def __init__(self, similarity_threshold: float = 0.7):
        """
        Initialize the model with similarity threshold.
        
        Args:
            similarity_threshold: Threshold above which products are considered similar (0.0 to 1.0)
        """
        self.similarity_threshold = similarity_threshold
        self.vectorizer = None
        self.product_vectors = None
        self.product_names = None
        self.similarity_matrix = None
        self.product_groups = None
        self.group_representatives = None
        
    def preprocess_product_name(self, name: str) -> str:
        """
        Clean and preprocess product names for better similarity matching.
        Handles specific patterns like 80.00gm, 1000.00ml, 1.00pcs, etc.
        
        Args:
            name: Raw product name
            
        Returns:
            Cleaned product name
        """
        if pd.isna(name):
            return ""
            
        # Convert to lowercase
        name = name.lower()
        
        # Remove specific measurement patterns found in your data
        patterns_to_remove = [
            # Handle decimal measurements like 80.00gm, 1000.00ml, 21.00gm
            r'\b\d+\.\d+\s*(gm|ml|kg|mg|oz|lbs?|pcs?|pieces?|g)\b',
            
            # Handle integer measurements like 250ml, 20pcs, 10s
            r'\b\d+\s*(gm|ml|kg|mg|oz|lbs?|pcs?|pieces?|g)\b',
            
            # Handle measurements without decimal like 250ml, 20pcs
            r'\b\d+\s*(ml|mg|g|gm|kg|oz|lbs?|pcs?|pieces?|pack|box|can|bottle|packet)\b',
            
            # Handle combinations like "250ml x 6", "10 x 20gm"
            r'\b\d+(\.\d+)?\s*x\s*\d+(\.\d+)?\s*(ml|mg|g|gm|kg|oz|lbs?|pcs?)\b',
            
            # Handle size indicators
            r'\b(small|medium|large|xl|xs|s|m|l)\b',
            
            # Handle standalone numbers like "10s", "20", "250"
            r'\b\d+s?\b',
            
            # Handle specific patterns like "10S", "20pcs"
            r'\b\d+[sS]\b',
            
            # Handle weight/volume without units that might be left
            r'\b\d+(\.\d+)?\b',
        ]
        
        for pattern in patterns_to_remove:
            name = re.sub(pattern, '', name)
        
        # Remove common packaging terms
        packaging_terms = [
            r'\b(can|bottle|packet|pack|box|carton|tin|jar|tube|sachet)\b',
            r'\b(energy|drink|cigarette|cigarettes)\b'  # Keep core product type
        ]
        
        # Don't remove these, but normalize them
        name = re.sub(r'\bcigarette\b', 'cigarette', name)
        name = re.sub(r'\bcigarettes\b', 'cigarette', name)
        name = re.sub(r'\benergy drink\b', 'energy drink', name)
        
        # Remove extra spaces and special characters except spaces
        name = re.sub(r'[^\w\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def extract_features(self, products: List[str]) -> np.ndarray:
        """
        Extract TF-IDF features from product names.
        
        Args:
            products: List of product names
            
        Returns:
            TF-IDF feature matrix
        """
        # Preprocess product names
        processed_products = [self.preprocess_product_name(name) for name in products]
        
        # Initialize TF-IDF vectorizer
        self.vectorizer = TfidfVectorizer(
            max_features=1000,
            ngram_range=(1, 3),  # Use 1-3 grams for better context
            stop_words='english',
            min_df=1,  # Minimum document frequency
            max_df=0.8,  # Maximum document frequency
            lowercase=True,
            token_pattern=r'\b[a-zA-Z][a-zA-Z]+\b'  # Only alphabetic tokens
        )
        
        # Fit and transform the data
        self.product_vectors = self.vectorizer.fit_transform(processed_products)
        
        return self.product_vectors
    
    def calculate_similarity_matrix(self) -> np.ndarray:
        """
        Calculate cosine similarity matrix between all products.
        
        Returns:
            Similarity matrix
        """
        if self.product_vectors is None:
            raise ValueError("Please extract features first using extract_features()")
            
        self.similarity_matrix = cosine_similarity(self.product_vectors)
        return self.similarity_matrix
    
    def find_similar_products(self, product_index: int, top_k: int = 10) -> List[Tuple[int, str, float]]:
        """
        Find top-k similar products for a given product.
        
        Args:
            product_index: Index of the product to find similarities for
            top_k: Number of similar products to return
            
        Returns:
            List of tuples (index, product_name, similarity_score)
        """
        if self.similarity_matrix is None:
            self.calculate_similarity_matrix()
            
        similarities = self.similarity_matrix[product_index]
        
        # Get top-k similar products (excluding the product itself)
        similar_indices = np.argsort(similarities)[::-1][1:top_k+1]
        
        results = []
        for idx in similar_indices:
            if similarities[idx] >= self.similarity_threshold:
                results.append((idx, self.product_names[idx], similarities[idx]))
                
        return results
    
    def group_similar_products(self) -> Dict[int, List[int]]:
        """
        Group similar products using clustering.
        
        Returns:
            Dictionary mapping group_id to list of product indices
        """
        if self.similarity_matrix is None:
            self.calculate_similarity_matrix()
        
        # Convert similarity to distance matrix for DBSCAN
        distance_matrix = 1 - self.similarity_matrix
        
        # Use DBSCAN clustering
        eps = 1 - self.similarity_threshold  # Convert similarity threshold to distance
        clustering = DBSCAN(eps=eps, min_samples=1, metric='precomputed')
        cluster_labels = clustering.fit_predict(distance_matrix)
        
        # Group products by cluster
        self.product_groups = {}
        for idx, label in enumerate(cluster_labels):
            if label not in self.product_groups:
                self.product_groups[label] = []
            self.product_groups[label].append(idx)
        
        return self.product_groups
    
    def get_group_representatives(self) -> Dict[int, Tuple[int, str]]:
        """
        Get representative product for each group (the one with highest average similarity).
        
        Returns:
            Dictionary mapping group_id to (product_index, product_name)
        """
        if self.product_groups is None:
            self.group_similar_products()
            
        self.group_representatives = {}
        
        for group_id, product_indices in self.product_groups.items():
            if len(product_indices) == 1:
                # Single product group
                idx = product_indices[0]
                self.group_representatives[group_id] = (idx, self.product_names[idx])
            else:
                # Find product with highest average similarity to other products in group
                best_idx = None
                best_avg_similarity = -1
                
                for idx in product_indices:
                    avg_similarity = np.mean([self.similarity_matrix[idx][other_idx] 
                                            for other_idx in product_indices if other_idx != idx])
                    if avg_similarity > best_avg_similarity:
                        best_avg_similarity = avg_similarity
                        best_idx = idx
                
                self.group_representatives[group_id] = (best_idx, self.product_names[best_idx])
        
        return self.group_representatives
    
    def fit(self, products: List[str]) -> 'ProductSimilarityModel':
        """
        Fit the model on product data.
        
        Args:
            products: List of product names
            
        Returns:
            Fitted model
        """
        self.product_names = products
        
        print(f"Processing {len(products)} products...")
        
        # Extract features
        self.extract_features(products)
        print("✓ Features extracted")
        
        # Calculate similarity matrix
        self.calculate_similarity_matrix()
        print("✓ Similarity matrix calculated")
        
        # Group similar products
        self.group_similar_products()
        print(f"✓ Products grouped into {len(self.product_groups)} groups")
        
        # Get group representatives
        self.get_group_representatives()
        print("✓ Group representatives identified")
        
        return self
    
    def get_consolidated_products(self, sales_data: pd.DataFrame, 
                                product_col: str = 'Product Name',
                                sales_col: str = 'Sales',
                                quantity_col: str = 'Quantity Sold') -> pd.DataFrame:
        """
        Consolidate similar products and aggregate their sales/quantities.
        
        Args:
            sales_data: DataFrame with product sales data
            product_col: Name of product column
            sales_col: Name of sales column
            quantity_col: Name of quantity column
            
        Returns:
            Consolidated DataFrame with grouped products
        """
        if self.product_groups is None or self.group_representatives is None:
            raise ValueError("Please fit the model first")
        
        # Create a mapping from product name to group representative
        product_to_representative = {}
        
        for group_id, product_indices in self.product_groups.items():
            representative_idx, representative_name = self.group_representatives[group_id]
            
            for product_idx in product_indices:
                original_product = self.product_names[product_idx]
                product_to_representative[original_product] = representative_name
        
        # Apply consolidation to sales data
        consolidated_data = sales_data.copy()
        consolidated_data['Consolidated_Product'] = consolidated_data[product_col].map(
            product_to_representative
        ).fillna(consolidated_data[product_col])
        
        # Aggregate sales and quantities by consolidated product
        aggregated_data = consolidated_data.groupby('Consolidated_Product').agg({
            sales_col: 'sum',
            quantity_col: 'sum'
        }).reset_index()
        
        # Rename columns
        aggregated_data.columns = ['Product Name', 'Sales', 'Quantity Sold']
        
        # Sort by sales descending
        aggregated_data = aggregated_data.sort_values('Sales', ascending=False).reset_index(drop=True)
        
        # Add serial number
        aggregated_data.insert(0, 'S.No', range(1, len(aggregated_data) + 1))
        
        return aggregated_data
    
    def save_model(self, filepath: str):
        """Save the trained model to disk."""
        model_data = {
            'vectorizer': self.vectorizer,
            'product_names': self.product_names,
            'product_groups': self.product_groups,
            'group_representatives': self.group_representatives,
            'similarity_threshold': self.similarity_threshold
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        print(f"Model saved to {filepath}")
    
    @classmethod
    def load_model(cls, filepath: str) -> 'ProductSimilarityModel':
        """Load a trained model from disk."""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        model = cls(model_data['similarity_threshold'])
        model.vectorizer = model_data['vectorizer']
        model.product_names = model_data['product_names']
        model.product_groups = model_data['product_groups']
        model.group_representatives = model_data['group_representatives']
        
        return model
    
    def analyze_groups(self) -> pd.DataFrame:
        """
        Analyze the groups formed and return summary statistics.
        
        Returns:
            DataFrame with group analysis
        """
        if self.product_groups is None:
            raise ValueError("Please fit the model first")
    
        analysis_data = []
        
        for group_id, product_indices in self.product_groups.items():
            representative_idx, representative_name = self.group_representatives[group_id]
            
            group_products = [self.product_names[idx] for idx in product_indices]
            
            analysis_data.append({
                'Group_ID': group_id,
                'Representative_Product': representative_name,
                'Group_Size': len(product_indices),
                'Products_in_Group': ' | '.join(group_products)
            })
        
        analysis_df = pd.DataFrame(analysis_data)
        analysis_df = analysis_df.sort_values('Group_Size', ascending=False).reset_index(drop=True)
        
        return analysis_df


# Example usage and training function
def train_product_similarity_model(product_data: pd.DataFrame, 
                                 product_col: str = 'Product Name',
                                 similarity_threshold: float = 0.7) -> ProductSimilarityModel:
    """
    Train the product similarity model on your product data.
    
    Args:
        product_data: DataFrame containing product information
        product_col: Column name containing product names
        similarity_threshold: Similarity threshold for grouping
        
    Returns:
        Trained ProductSimilarityModel
    """
    # Get unique product names
    unique_products = product_data[product_col].dropna().unique().tolist()
    
    print(f"Training model on {len(unique_products)} unique products...")
    
    # Initialize and train model
    model = ProductSimilarityModel(similarity_threshold=similarity_threshold)
    model.fit(unique_products)
    
    return model


# Integration with your existing product performance module
def get_consolidated_product_performance(similarity_model: ProductSimilarityModel,
                                       default_start_date=None, 
                                       default_end_date=None) -> pd.DataFrame:
    """
    Get consolidated product performance using the similarity model.
    This replaces your original fetch_product_data function.
    """
    from queries.product_performance import fetch_product_data
    
    # Get original product data
    original_data = fetch_product_data(default_start_date, default_end_date)
    
    if original_data.empty:
        return original_data
    
    # Extract numeric values for consolidation
    def extract_numeric_value(value_str):
        if pd.isna(value_str):
            return 0
        return float(str(value_str).split()[0].replace(',', ''))
    
    # Prepare data for consolidation
    consolidation_data = original_data.copy()
    consolidation_data['Sales_Numeric'] = consolidation_data['Sales'].apply(extract_numeric_value)
    consolidation_data['Quantity_Numeric'] = consolidation_data['Quantity Sold'].apply(extract_numeric_value)
    
    # Get consolidated results
    consolidated_data = similarity_model.get_consolidated_products(
        consolidation_data,
        product_col='Product Name',
        sales_col='Sales_Numeric',
        quantity_col='Quantity_Numeric'
    )
    
    return consolidated_data