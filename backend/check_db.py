#!/usr/bin/env python
"""Check database contents and structure"""

from sqlalchemy import create_engine, text

engine = create_engine('sqlite:///./jobpulse.db')
with engine.connect() as conn:
    # Get all tables
    result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
    print('\n=== Database Tables ===')
    tables = []
    for row in result:
        table_name = row[0]
        tables.append(table_name)
        print(f'  - {table_name}')
    
    # Count records
    print('\n=== Record Counts ===')
    for table in tables:
        result = conn.execute(text(f'SELECT COUNT(*) as count FROM {table}'))
        count = result.fetchone()[0]
        print(f'  {table}: {count} records')
    
    # Show sample data
    print('\n=== Sample Skills ===')
    result = conn.execute(text('SELECT id, name, category FROM skill LIMIT 5'))
    for row in result:
        print(f'  {row[1]} ({row[2]})')
    
    print('\n=== Sample Jobs ===')
    result = conn.execute(text('SELECT id, title, company, description FROM job LIMIT 3'))
    for row in result:
        print(f'  [{row[1]}] @ {row[2]}')
        print(f'    Description: {row[3][:100]}...')
    
    print('\n=== Job-Skill Mappings ===')
    result = conn.execute(text('SELECT COUNT(*) as count FROM job_skill'))
    count = result.fetchone()[0]
    print(f'  Total job-skill associations: {count}')
    
    print('\n=== Skill Snapshots ===')
    result = conn.execute(text('SELECT COUNT(*) as count FROM skill_snapshot'))
    count = result.fetchone()[0]
    print(f'  Total skill snapshots: {count}')
