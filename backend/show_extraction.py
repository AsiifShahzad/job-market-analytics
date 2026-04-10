#!/usr/bin/env python
"""Show how skills are extracted from job descriptions"""

from sqlalchemy import create_engine, text

engine = create_engine('sqlite:///./jobpulse.db')
with engine.connect() as conn:
    # Get a sample job with its extracted skills
    print('\n=== SAMPLE JOB AD ===')
    result = conn.execute(text('''
        SELECT id, title, company, description, salary_min, salary_max, city, country
        FROM job LIMIT 1
    '''))
    job = result.fetchone()
    
    print(f'\nJob Title: {job[1]}')
    print(f'Company: {job[2]}')
    print(f'Location: {job[6]}, {job[7]}')
    print(f'Salary: ${job[4]:,.0f} - ${job[5]:,.0f}')
    print(f'\nDescription:\n{job[3][:500]}...\n')
    
    # Get skills extracted from this job
    print('=== SKILLS EXTRACTED FROM THIS JOB ===')
    result = conn.execute(text(f'''
        SELECT s.name, s.category
        FROM skill s
        JOIN job_skill js ON s.id = js.skill_id
        WHERE js.job_id = '{job[0]}'
        ORDER BY s.name
    '''))
    
    skills = result.fetchall()
    print(f'Total skills found: {len(skills)}\n')
    
    # Group by category
    by_category = {}
    for skill_name, category in skills:
        if category not in by_category:
            by_category[category] = []
        by_category[category].append(skill_name)
    
    for category, skill_list in sorted(by_category.items()):
        print(f'{category.upper()}:')
        for skill in sorted(skill_list):
            print(f'  - {skill}')
        print()
    
    # Show top skills across all jobs
    print('\n=== TOP 15 SKILLS ACROSS ALL JOBS ===')
    result = conn.execute(text('''
        SELECT s.name, COUNT(js.job_id) as job_count
        FROM skill s
        JOIN job_skill js ON s.id = js.skill_id
        GROUP BY s.id, s.name
        ORDER BY job_count DESC
        LIMIT 15
    '''))
    
    for i, (skill, count) in enumerate(result.fetchall(), 1):
        print(f'{i:2}. {skill:<20} - {count} jobs')
    
    # Show skill extraction stats
    print('\n=== SKILL EXTRACTION STATISTICS ===')
    result = conn.execute(text('''
        SELECT 
            COUNT(DISTINCT j.id) as total_jobs,
            COUNT(DISTINCT s.id) as unique_skills,
            COUNT(*) as total_extractions,
            ROUND(CAST(COUNT(*) AS FLOAT) / COUNT(DISTINCT j.id), 2) as avg_skills_per_job
        FROM job j
        LEFT JOIN job_skill js ON j.id = js.job_id
        LEFT JOIN skill s ON js.skill_id = s.id
    '''))
    
    stats = result.fetchone()
    print(f'Total jobs processed: {stats[0]}')
    print(f'Unique skills found: {stats[1]}')
    print(f'Total skill extractions: {stats[2]}')
    print(f'Average skills per job: {stats[3]}')
