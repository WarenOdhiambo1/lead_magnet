import psycopg2
from psycopg2.extras import RealDictCursor
import json

def inspect_database():
    conn = psycopg2.connect(
        host="localhost",
        database="student_finance_dream",
        user="Waren_Dev",
        password="Wa41re8790018750.."
    )
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print("=" * 80)
    print("DATABASE SCHEMA INSPECTION: student_finance_dream")
    print("=" * 80)
    print()
    
    # Get all tables
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name
    """)
    tables = [row['table_name'] for row in cur.fetchall()]
    
    print(f"📊 TOTAL TABLES: {len(tables)}")
    print()
    
    schema = {}
    
    for table in tables:
        print(f"┌─ TABLE: {table}")
        print("│")
        
        # Get columns
        cur.execute(f"""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_name = '{table}'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        
        # Get primary keys
        cur.execute(f"""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{table}'::regclass AND i.indisprimary
        """)
        pks = [row['attname'] for row in cur.fetchall()]
        
        # Get foreign keys
        cur.execute(f"""
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = '{table}'
        """)
        fks = cur.fetchall()
        
        # Get row count
        cur.execute(f"SELECT COUNT(*) as count FROM {table}")
        row_count = cur.fetchone()['count']
        
        schema[table] = {
            'columns': [],
            'primary_keys': pks,
            'foreign_keys': [],
            'row_count': row_count
        }
        
        print(f"│  📈 Row Count: {row_count}")
        print("│")
        print("│  COLUMNS:")
        
        for col in columns:
            col_name = col['column_name']
            data_type = col['data_type']
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            default = f"DEFAULT {col['column_default']}" if col['column_default'] else ""
            
            pk_marker = " 🔑 PK" if col_name in pks else ""
            fk_info = ""
            for fk in fks:
                if fk['column_name'] == col_name:
                    fk_info = f" 🔗 FK → {fk['foreign_table_name']}.{fk['foreign_column_name']}"
                    schema[table]['foreign_keys'].append({
                        'column': col_name,
                        'references': f"{fk['foreign_table_name']}.{fk['foreign_column_name']}"
                    })
            
            max_len = f"({col['character_maximum_length']})" if col['character_maximum_length'] else ""
            
            schema[table]['columns'].append({
                'name': col_name,
                'type': data_type + max_len,
                'nullable': col['is_nullable'] == 'YES',
                'default': col['column_default'],
                'is_pk': col_name in pks,
                'fk_reference': fk_info.replace(" 🔗 FK → ", "") if fk_info else None
            })
            
            print(f"│    • {col_name:<30} {data_type}{max_len:<15} {nullable:<10} {default}{pk_marker}{fk_info}")
        
        print("│")
        print("└" + "─" * 78)
        print()
    
    # Save to JSON
    with open('database_schema.json', 'w') as f:
        json.dump(schema, f, indent=2, default=str)
    
    print()
    print("=" * 80)
    print("RELATIONSHIPS SUMMARY")
    print("=" * 80)
    print()
    
    for table, info in schema.items():
        if info['foreign_keys']:
            print(f"📦 {table}")
            for fk in info['foreign_keys']:
                print(f"   └─ {fk['column']} → {fk['references']}")
            print()
    
    print()
    print("=" * 80)
    print("✅ Schema exported to: database_schema.json")
    print("=" * 80)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_database()
