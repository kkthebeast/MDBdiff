import pyodbc
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import csv
import xml.etree.ElementTree as ET
import yaml

# Add this function near the top of the file, after imports
def get_normalized_type(type_obj):
    mapping = {
        int: 'integer',
        float: 'float',
        str: 'string',
        bool: 'boolean'
    }
    if type_obj in mapping:
        return mapping[type_obj]
    if hasattr(type_obj, '__name__'):
        return type_obj.__name__
    return str(type_obj)

def get_schema_version(mdb_path):
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={mdb_path};"
    )
    try:
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        UPDATE_VERSION,
                        UPDATE_NUM,
                        UPDATE_DAY,
                        UPDATE_MONTH,
                        UPDATE_YEAR 
                    FROM DATABASE_INFO 
                    ORDER BY UPDATE_NUM DESC
                """)
                row = cursor.fetchone()
                if row:
                    return {
                        'version': row[0],
                        'build': row[1],
                        'date': f"{row[2]}/{row[3]}/{row[4]}"
                    }
                return {
                    'version': "Unknown",
                    'build': "Unknown",
                    'date': "Unknown"
                }
    except:
        return {
            'version': "Unknown",
            'build': "Unknown",
            'date': "Unknown"
        }

# Helper to extract schema from a database
def get_schema(mdb_path):
    schema = {}
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={mdb_path};"
    )
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            # Get all user tables
            tables = [t.table_name for t in cursor.tables(tableType='TABLE') 
                     if not t.table_name.startswith('MSys')]
            
            for table_name in tables:
                # Get column information
                cursor.execute(f"SELECT * FROM [{table_name}] WHERE 1=0")
                columns = []
                for column in cursor.description:
                    col_name = column[0]
                    col_type = column[1].__name__ if hasattr(column[1], '__name__') else str(column[1])
                    
                    # Normalize Access types
                    type_mapping = {
                        'str': 'TEXT',
                        'int': 'INTEGER',
                        'float': 'FLOAT',
                        'bool': 'BOOLEAN',
                        'datetime': 'DATETIME',
                        'date': 'DATE',
                        'time': 'TIME',
                        'binary': 'BINARY',
                        'decimal': 'DECIMAL'
                    }
                    
                    col_type = type_mapping.get(col_type.lower(), col_type.upper())
                    
                    columns.append({
                        'name': col_name,
                        'type': col_type
                    })
                
                schema[table_name] = columns
    
    return schema

# Helper to diff two schemas
def diff_schemas(schema_a, schema_b):
    diff = {
        'tables_added': [],
        'tables_removed': [],
        'tables_modified': {}
    }

    tables_a = set(schema_a.keys())
    tables_b = set(schema_b.keys())

    # Find added and removed tables
    diff['tables_added'] = sorted(list(tables_b - tables_a))
    diff['tables_removed'] = sorted(list(tables_a - tables_b))

    # Compare tables present in both schemas
    for table in sorted(tables_a & tables_b):
        cols_a = {col['name']: col['type'] for col in schema_a[table]}
        cols_b = {col['name']: col['type'] for col in schema_b[table]}

        col_names_a = set(cols_a.keys())
        col_names_b = set(cols_b.keys())

        added = sorted(list(col_names_b - col_names_a))
        removed = sorted(list(col_names_a - col_names_b))
        changed = []

        # Check for type changes in common columns
        for col in sorted(col_names_a & col_names_b):
            if cols_a[col] != cols_b[col]:
                changed.append({
                    'column': col,
                    'type_a': cols_a[col],
                    'type_b': cols_b[col]
                })

        if added or removed or changed:
            diff['tables_modified'][table] = {
                'columns_added': added,
                'columns_removed': removed,
                'columns_changed': changed
            }

    return diff

# Export helpers
def export_to_txt(diff, path, version_a, version_b, schema_a, schema_b, show_types=True):
    with open(path, 'w') as f:
        f.write(f"Database A Version: {version_a['version']} (Build {version_a['build']}) - {version_a['date']}\n")
        f.write(f"Database B Version: {version_b['version']} (Build {version_b['build']}) - {version_b['date']}\n\n")
        f.write("Tables Added:\n")
        for table in diff['tables_added']:
            f.write(f"  + {table}\n")
            if show_types:
                for col in schema_b[table]:
                    f.write(f"    Column: {col['name']} (Type: {col['type']})\n")
            else:
                for col in schema_b[table]:
                    f.write(f"    Column: {col['name']}\n")

        f.write("\nTables Removed:\n")
        for table in diff['tables_removed']:
            f.write(f"  - {table}\n")
            if show_types:
                for col in schema_a[table]:
                    f.write(f"    Column: {col['name']} (Type: {col['type']})\n")
            else:
                for col in schema_a[table]:
                    f.write(f"    Column: {col['name']}\n")

        f.write("\nTables Modified:\n")
        for table, changes in diff['tables_modified'].items():
            f.write(f"  * {table}:\n")
            for col in changes['columns_added']:
                col_type = next(c['type'] for c in schema_b[table] if c['name'] == col)
                f.write(f"    + Column Added: {col} (Type: {col_type})\n")
            for col in changes['columns_removed']:
                col_type = next(c['type'] for c in schema_a[table] if c['name'] == col)
                f.write(f"    - Column Removed: {col} (Type: {col_type})\n")
            for col in changes['columns_changed']:
                f.write(f"    ~ Column Changed: {col['column']} (Type: {col['type_a']} -> {col['type_b']})\n")

def export_to_csv(diff, path, version_a, version_b, schema_a, schema_b, show_types=True):
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Schema Information'])
        writer.writerow(['Database A Version', f"{version_a['version']} (Build {version_a['build']}) - {version_a['date']}"])
        writer.writerow(['Database B Version', f"{version_b['version']} (Build {version_b['build']}) - {version_b['date']}"])
        writer.writerow([])
        if show_types:
            writer.writerow(['Change Type', 'Table', 'Column', 'Data Type', 'Detail'])
        else:
            writer.writerow(['Change Type', 'Table', 'Column', 'Detail'])

        for table in diff['tables_added']:
            writer.writerow(['Table Added', table, '', '', ''])
            if show_types:
                for col in schema_b[table]:
                    writer.writerow(['', '', col['name'], col['type'], '(New)'])
            else:
                for col in schema_b[table]:
                    writer.writerow(['', '', col['name'], '', '(New)'])

        for table in diff['tables_removed']:
            writer.writerow(['Table Removed', table, '', '', ''])
            if show_types:
                for col in schema_a[table]:
                    writer.writerow(['', '', col['name'], col['type'], '(Removed)'])
            else:
                for col in schema_a[table]:
                    writer.writerow(['', '', col['name'], '', '(Removed)'])

        for table, changes in diff['tables_modified'].items():
            for col in changes['columns_added']:
                col_type = next(c['type'] for c in schema_b[table] if c['name'] == col)
                writer.writerow(['Column Added', table, col, col_type, ''])
            for col in changes['columns_removed']:
                col_type = next(c['type'] for c in schema_a[table] if c['name'] == col)
                writer.writerow(['Column Removed', table, col, col_type, ''])
            for col in changes['columns_changed']:
                detail = f"{col['type_a']} -> {col['type_b']}"
                writer.writerow(['Column Changed', table, col['column'], '', detail])

def export_to_xml(diff, path, version_a, version_b, schema_a, schema_b, show_types=True):
    root = ET.Element('SchemaDiff')
    
    # Add version information
    versions = ET.SubElement(root, 'SchemaVersions')
    db_a = ET.SubElement(versions, 'DatabaseA')
    ET.SubElement(db_a, 'Version').text = str(version_a['version'])
    ET.SubElement(db_a, 'Build').text = str(version_a['build'])
    ET.SubElement(db_a, 'Date').text = str(version_a['date'])
    
    db_b = ET.SubElement(versions, 'DatabaseB')
    ET.SubElement(db_b, 'Version').text = str(version_b['version'])
    ET.SubElement(db_b, 'Build').text = str(version_b['build'])
    ET.SubElement(db_b, 'Date').text = str(version_b['date'])

    added = ET.SubElement(root, 'TablesAdded')
    for table in diff['tables_added']:
        tnode = ET.SubElement(added, 'Table', name=table)
        if show_types:
            for col in schema_b[table]:
                cnode = ET.SubElement(tnode, 'Column', name=col['name'], type=col['type'])
        else:
            for col in schema_b[table]:
                ET.SubElement(tnode, 'Column', name=col['name'])

    removed = ET.SubElement(root, 'TablesRemoved')
    for table in diff['tables_removed']:
        tnode = ET.SubElement(removed, 'Table', name=table)
        if show_types:
            for col in schema_a[table]:
                cnode = ET.SubElement(tnode, 'Column', name=col['name'])
                cnode.set('type', col['type'])
        else:
            for col in schema_a[table]:
                ET.SubElement(tnode, 'Column', name=col['name'])

    modified = ET.SubElement(root, 'TablesModified')
    for table, changes in diff['tables_modified'].items():
        tnode = ET.SubElement(modified, 'Table', name=table)
        for col in changes['columns_added']:
            cnode = ET.SubElement(tnode, 'ColumnAdded', name=col)
            if show_types:
                col_type = next(c['type'] for c in schema_b[table] if c['name'] == col)
                cnode.set('type', col_type)
        for col in changes['columns_removed']:
            cnode = ET.SubElement(tnode, 'ColumnRemoved', name=col)
            if show_types:
                col_type = next(c['type'] for c in schema_a[table] if c['name'] == col)
                cnode.set('type', col_type)
        for col in changes['columns_changed']:
            cnode = ET.SubElement(tnode, 'ColumnChanged', name=col['column'])
            cnode.set('from', col['type_a'])
            cnode.set('to', col['type_b'])

    # Pretty print the XML
    from xml.dom import minidom
    xmlstr = minidom.parseString(ET.tostring(root)).toprettyxml(indent="    ")
    with open(path, 'w', encoding='utf-8') as f:
        f.write(xmlstr)

def export_to_yaml(diff, path, version_a, version_b, schema_a, schema_b, show_types=True):
    # Add version and schema info to diff dictionary
    diff_with_versions = {
        'schema_versions': {
            'database_a': version_a,
            'database_b': version_b
        }
    }
    
    if show_types:
        diff_with_versions['schemas'] = {
            'database_a': schema_a,
            'database_b': schema_b
        }
    
    diff_with_versions.update(diff)
    
    with open(path, 'w') as f:
        yaml.dump(diff_with_versions, f, default_flow_style=False, sort_keys=False, indent=2)

# GUI app
def run_gui():
    root = tk.Tk()
    root.title("MDB Schema Diff Tool")
    root.geometry("600x350")

    # Add show_types variable
    show_types = tk.BooleanVar(value=True)  # Default to showing types

    def browse_output(entry):
        filetypes = [
            ('Text Files', '*.txt'),
            ('CSV Files', '*.csv'),
            ('XML Files', '*.xml'),
            ('YAML Files', '*.yml')
        ]
        filename = filedialog.asksaveasfilename(
            defaultextension='.yml',
            filetypes=filetypes
        )
        if filename:
            # Check if extension is missing
            ext = os.path.splitext(filename)[1].lower()
            if not ext:
                # Get selected file type from dialog
                selected_type = filename.split(' ')[-1] if ' ' in filename else '.txt'
                filename += selected_type
            entry.delete(0, tk.END)
            entry.insert(0, filename)

    def browse_file(entry):
        filename = filedialog.askopenfilename(filetypes=[("Access DB", "*.mdb")])
        if filename:
            entry.delete(0, tk.END)
            entry.insert(0, filename)

    def run_diff():
        file_a = entry_a.get()
        file_b = entry_b.get()
        output_path = entry_output.get()

        if not all([file_a, file_b, output_path]):
            messagebox.showerror("Error", "Please select both MDB files and an output location.")
            return

        try:
            schema_a = get_schema(file_a)
            schema_b = get_schema(file_b)
            version_a = get_schema_version(file_a)
            version_b = get_schema_version(file_b)
            diff = diff_schemas(schema_a, schema_b)

            # Check if there are any differences
            has_differences = any([
                diff['tables_added'],
                diff['tables_removed'],
                diff['tables_modified']
            ])

            if not has_differences:
                proceed = messagebox.askyesno(
                    "No Differences Found",
                    "The schemas are identical. Do you still want to create an output file?"
                )
                if not proceed:
                    return

            fmt = os.path.splitext(output_path)[1].lower()
            
            # Pass show_types to export functions
            if fmt == ".txt":
                export_to_txt(diff, output_path, version_a, version_b, schema_a, schema_b, show_types.get())
            elif fmt == ".csv":
                export_to_csv(diff, output_path, version_a, version_b, schema_a, schema_b, show_types.get())
            elif fmt == ".xml":
                export_to_xml(diff, output_path, version_a, version_b, schema_a, schema_b, show_types.get())
            elif fmt == ".yml":
                export_to_yaml(diff, output_path, version_a, version_b, schema_a, schema_b, show_types.get())
            else:
                export_to_txt(diff, output_path, version_a, version_b, schema_a, schema_b, show_types.get())

            messagebox.showinfo("Done", f"Schema diff saved to {output_path}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    tk.Label(root, text="Database A:").pack(anchor='w', padx=10)
    entry_a = tk.Entry(root, width=80)
    entry_a.pack(padx=10)
    tk.Button(root, text="Browse", command=lambda: browse_file(entry_a)).pack(pady=2)

    tk.Label(root, text="Database B:").pack(anchor='w', padx=10)
    entry_b = tk.Entry(root, width=80)
    entry_b.pack(padx=10)
    tk.Button(root, text="Browse", command=lambda: browse_file(entry_b)).pack(pady=2)

    tk.Label(root, text="Save Output As:").pack(anchor='w', padx=10)
    entry_output = tk.Entry(root, width=80)
    entry_output.pack(padx=10)
    tk.Button(root, text="Browse", command=lambda: browse_output(entry_output)).pack(pady=2)

    # Add checkbox after the file entries but before the Run Diff button
    type_checkbox = tk.Checkbutton(
        root, 
        text="Include Data Types in Output", 
        variable=show_types,
        font=('Arial', 10)
    )
    type_checkbox.pack(pady=10)

    run_button = tk.Button(
        root, 
        text="Run Diff", 
        command=run_diff,
        font=('Arial', 10, 'bold'),
        bg='#4CAF50',
        fg='white',
        width=20,
        height=2
    )
    run_button.pack(pady=20)  # Increased padding

    root.mainloop()

if __name__ == '__main__':
    run_gui()

# Enjoy! -KK