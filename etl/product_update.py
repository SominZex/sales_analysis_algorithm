import psycopg2

# Database connection details
DB_HOST = "server_ip"
DB_NAME = "db_name"
DB_USER = "user_name"
DB_PASSWORD = "password"

queries = [
    # === Product updates ===
    """UPDATE product_sales SET "productname" = 'Cadbury Dairy Milk Silk Fruit & Nut Chocolate 55g' WHERE "productname" = 'Cadbury Dairy Milk Silk Fruit & Nut Chocolate Bar 55g';""",
    """UPDATE product_sales SET "productname" = 'Ocean Peach & Passion Fruit Fruit Drink 500 ml' WHERE "productname" = 'Ocean Peach & Passion Fruit Flavour Fruit Drink 500 ml';""",
    """UPDATE product_sales SET "productname" = 'Lotus Biscoff Original Biscuit-250 gm' WHERE "productname" = 'Lotus Biscoff Original Caramelised Biscuit - 250 gm';""",
    """UPDATE product_sales SET "productname" = 'Labubu Have A Seat Original Monsters Blind' WHERE "productname" = 'Labubu Have A Seat Original Popmart The Monsters Blind';""",
    """UPDATE product_sales SET "productname" = 'Kwality Wall''s Magnum Chocolate Ice Cream 70ml' WHERE "productname" = 'Kwality Wall''s Magnum Chocolate Truffle Ice Cream 70ml';""",
    """UPDATE product_sales SET "productname" = 'Cadbury Dairy Milk Silk Fruit & Nut Bar 137g' WHERE "productname" = 'Cadbury Dairy Milk Silk Fruit & Nut Chocolate Bar 137g';""",
    """UPDATE product_sales SET "productname" = 'Nutella & Go Hazelnut Spread & Pretzels 48gm' WHERE "productname" = 'Nutella & Go Hazelnut Spread & Pretzels Sticks, 48gm';""",
    """UPDATE product_sales SET "productname" = 'Coca Cola Diet Coke Carbonated Soft Drink 300ml' WHERE "productname" = 'Coca Cola Diet Coke Carbonated Soft Drink Can 300ml';""",
    """UPDATE product_sales SET "productname" = 'Coca Cola Soft Drink Original Taste, 1L ' WHERE "productname" = 'Coca Cola Soft Drink Original Taste, Refreshing, 1L ';""",
    """UPDATE product_sales SET "productname" = 'Ocean Strawberry & Lime Flavour Drink 500 ml ' WHERE "productname" = 'Ocean Strawberry & Lime Flavour Fruit Drink 500 ml';""",
    """UPDATE product_sales SET "productname" = 'Cadbury Dairy Milk Silk Roast Almond Chocolate 58g' WHERE "productname" = 'Cadbury Dairy Milk Silk Roast Almond Chocolate Bar 58g';""",
    """UPDATE product_sales SET "productname" = 'Cadbury Celebrations Assorted Chocolate 154.2g' WHERE "productname" = 'Cadbury Celebrations Assorted Chocolate Gift Pack 154.2g ';""",
    """UPDATE product_sales SET "productname" = 'Godiva Chocolate Milk Chocolate Hazelnut 83g' WHERE "productname" = 'Godiva Chocolate Milk Chocolate Hazelnut Oyster 83 G';""",
    """UPDATE product_sales SET "productname" = 'Cadbury Dairy Milk Silk Roast Almond Choco 58g' WHERE "productname" = 'Cadbury Dairy Milk Silk Roast Almond Chocolate 58g';""",
    """UPDATE product_sales SET "productname" = 'Cadbury Dairy Milk Silk Hazelnut Chocolate 58g' WHERE "productname" = 'Cadbury Dairy Milk Silk Hazelnut Chocolate Bar 58g';""",
    """UPDATE product_sales SET "productname" = 'MR. MAKHANA Popped Lotus Seeds-Pudina Party 75g' WHERE "productname" = 'MR. MAKHANA Popped Lotus Seeds - Pudina Party 75 g';""",
    """UPDATE product_sales SET "productname" = 'Coca Cola Diet Coke Carbonated Drink 300ml' WHERE "productname" = 'Coca Cola Diet Coke Carbonated Soft Drink 300ml';""",
    """UPDATE product_sales SET "productname" = 'Cadbury Dairy Milk Silk Chocolate Bar 112g' WHERE "productname" = 'Cadbury Dairy Milk Silk Bubbly Chocolate Bar 112g';""",
    """UPDATE product_sales SET "productname" = 'Catch Flavoured Water - Lemon N Lime 750 Ml' WHERE "productname" = 'Catch Flavoured Water - Lemon N Lime 750 Ml Bottle';""",
    """UPDATE product_sales SET "productname" = 'MrBeast Almond Chocolate with Almond 60g' WHERE "productname" = 'MrBeast Feastables Almond Milk Chocolate with Almond Chunks Bar 60g';""",
    """UPDATE product_sales SET "productname" = 'Samyang Chicken Flavor Ramen Buldak Noodles 650Gm' WHERE "productname" = 'Samyang Hot Chicken Flavor Ramen Buldak Carbonara Noodles 650Gm ';""",
    """UPDATE product_sales SET "productname" = 'Parle Platina Hide & Seek Chocolate Cookies-100gm' WHERE "productname" = 'Parle Platina Hide & Seek Chocolate Chip Cookies - 100gm';""",
    """UPDATE product_sales SET "productname" = 'Monster Energy Drink Zero Sugar 12 x 500ml' WHERE "productname" = 'Monster Energy Drink Ultra Zero Sugar 12 x 500ml';""",
    """UPDATE product_sales SET "productname" = 'Nestle Munch Max Chocolate Coated Wafer 38.5g' WHERE "productname" = 'Nestle Munch Max Chocolate Coated Crunchy Wafer Bar 38.5g';""",
    """UPDATE product_sales SET "productname" = 'RiteBite Max Protein Bar Ultimate Choco 100g' WHERE "productname" = 'RiteBite Max Protein Bar Ultimate Choco Berry 100g';""",
    """UPDATE product_sales SET "productname" = 'Sprite Clear Carbonated Drink Bottle 750ml' WHERE "productname" = 'Sprite Clear Carbonated Drink Pet Bottle 750ml';""",
    """UPDATE product_sales SET "productname" = 'Catch Flavoured Water - Black Currant 750 Ml' WHERE "productname" = 'Catch Flavoured Water - Black Currant 750 Ml Bottle';""",
    """UPDATE product_sales SET "productname" = 'Coca Cola Soft Drink Original Taste 750Ml' WHERE "productname" = 'Coca Cola Soft Drink Original Taste, Refreshing, 750Ml';""",
    """UPDATE product_sales SET "productname" = 'Cadbury Dairy Milk Silk Fruit & Nut 55g' WHERE "productname" = 'Cadbury Dairy Milk Silk Fruit & Nut Chocolate 55g';""",
    """UPDATE product_sales SET "productname" = 'iteBite Max Protein Bar Choco Almond 30g' WHERE "productname" = 'iteBite Max Protein Bar Ultimate Choco Almond 30g';""",
    """UPDATE product_sales SET "productname" = 'Epigamia Chocolate Protein Milkshake, 250 ml' WHERE "productname" = 'Epigamia Chocolate Turbo 25 g Protein Milkshake, 250 ml';""",
    """UPDATE product_sales SET "productname" = 'Mr Makhana Himalaya Lotus Seeds 60g' WHERE "productname" = 'Mr Makhana Himalaya Salt & Pepper Popped Lotus Seeds 60g';""",
    """UPDATE product_sales SET "productname" = 'Cadbury Dairy Milk Fruit & Nut Chocolate 51 g' WHERE "productname" = 'Cadbury Dairy Milk Silk Fruit & Nut Chocolate Bar 51 g';""",
    """UPDATE product_sales SET "productname" = 'RiteBite Max Protein Bar Choco Almond 30g' WHERE "productname" = 'RiteBite Max Protein Bar Ultimate Choco Almond 30g';""",
    """UPDATE product_sales SET "productname" = 'Toblerone Swiss Milk Chocolate-Honey Almond 100gm' WHERE "productname" = 'Toblerone Swiss Milk Chocolate - With Honey & Almond Nougat, 100gm';""",
    """UPDATE product_sales SET "productname" = 'Cetaphil Sun Pff 50+ Light Gel Skin Body 50 Ml' WHERE "productname" = 'Cetaphil Sun Pff 50+ Light Gel Sensitive Skin Body 50 Ml';""",
    """UPDATE product_sales SET "productname" = 'MrBeast Feastables Milk Chocolate Rice Bar 60g' WHERE "productname" = 'MrBeast Feastables Crunch Milk Chocolate with Puffed Rice Bar 60g';""",

    # === Store updates ===
    """UPDATE store_sales SET "storename" = 'Daryaganj Subhash Marg' WHERE "storename" = 'Daryaganj Netaji Subhash Marg';""",
    """UPDATE billing_data SET "storeName" = 'Daryaganj Subhash Marg' WHERE "storeName" = 'Daryaganj Netaji Subhash Marg';"""
]

def run_updates():
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        for q in queries:
            cur.execute(q)
        conn.commit()
        print("All updates applied successfully")
        cur.close()
    except Exception as e:
        print(f"Error running updates: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_updates()
