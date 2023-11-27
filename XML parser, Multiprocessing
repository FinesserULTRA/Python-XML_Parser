import mysql.connector
import multiprocessing
import time
import xml.etree.ElementTree as ET

start_time = time.perf_counter()

#table names go here
record_types = [
    "article",
    "www",
    "inproceedings",
    "book",
    "phdthesis",
    "proceedings",
    "mastersthesis",
    "incollection",
    "data"
]

num_cpus = multiprocessing.cpu_count()
chunk_size = 50000  # Adjust the chunk size as needed


def create_columns_tables(current_chunk):
    local_cnx = get_connection()
    local_cursor = local_cnx.cursor()

    for child in current_chunk:
        try:
            local_cursor.execute(f"CREATE TABLE IF NOT EXISTS `{child.tag}` (id INT AUTO_INCREMENT PRIMARY KEY)")
            for de in child:
                try:
                    local_cursor.execute(f"ALTER TABLE `{child.tag}` ADD COLUMN {de.tag} varchar(255)")
                except mysql.connector.Error as err:
                    pass
        except mysql.connector.Error as err:
            print(err)
    local_cnx.commit()


# Define a function to process individual subroot elements
def process_subroot(subroot_element):
    tag = {}
    for col in subroot_element:
        tag[col.tag] = col.text

    columns = '`, `'.join(tag.keys())
    
    query = f"INSERT INTO `{subroot_element.tag}` (`{columns}`) VALUES {tuple(tag.values())}"

    return query


# Path to your XML file
xml_file_path = "smol.xml"


# Database connection function for each process
def get_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        database='test'
    )


# Function for multiprocessing processing
def process_chunk(chunk):
    try:
        local_cnx = get_connection()
        local_cursor = local_cnx.cursor()

        for child in chunk:
            try:
                query = process_subroot(child)
                local_cursor.execute(query)

            except mysql.connector.Error as err:
                print(err)

        local_cnx.commit()
        local_cursor.close()
        local_cnx.close()

    except mysql.connector.Error as err:
        print("Error:", err)


if __name__ == "__main__":

    # Create a multiprocessing Pool
    with multiprocessing.Pool(processes=4) as pool:
        tree = ET.iterparse(xml_file_path, events=("start", "end"))
        current_chunk = []
        _, root = next(tree)

        for event, element in tree:
            if event == "end" and element in root:
                current_chunk.append(element)
                if len(current_chunk) >= chunk_size:
                    pool.apply_async(create_columns_tables, (current_chunk,))
                    pool.apply_async(process_chunk, (current_chunk,))
                    current_chunk = []

        # Process any remaining elements in the last chunk
        if current_chunk:
            pool.apply_async(create_columns_tables, (current_chunk,))
            pool.apply_async(process_chunk, (current_chunk,))

        pool.close()
        pool.join()

    print("Processing completed.")
    end_time = time.perf_counter()
    total_time = round(end_time - start_time, 2)
    print("Run Time: ", total_time)
