/**
 * Filtering utilities for jobs and skills
 */

/**
 * Filter jobs by array of skills (ANY or ALL match)
 */
export const filterJobsBySkills = (jobs, skills, matchType = 'any') => {
  if (!skills || skills.length === 0) return jobs

  return jobs.filter((job) => {
    const jobSkills = job.required_skills || []
    const skillsLower = skills.map((s) => s.toLowerCase())
    const jobSkillsLower = jobSkills.map((s) => s.toLowerCase())

    if (matchType === 'all') {
      return skillsLower.every((skill) =>
        jobSkillsLower.some((jobSkill) => jobSkill.includes(skill))
      )
    } else {
      // matchType === 'any'
      return skillsLower.some((skill) =>
        jobSkillsLower.some((jobSkill) => jobSkill.includes(skill))
      )
    }
  })
}

/**
 * Filter jobs by salary range
 */
export const filterJobsBySalary = (jobs, minSalary, maxSalary) => {
  return jobs.filter((job) => {
    if (!job.salary) return true // Keep jobs without salary info

    const salary = job.salary
    if (minSalary && salary < minSalary) return false
    if (maxSalary && salary > maxSalary) return false
    return true
  })
}

/**
 * Filter jobs by keyword in title or description
 */
export const filterJobsByKeyword = (jobs, keyword) => {
  if (!keyword) return jobs

  const keywordLower = keyword.toLowerCase()
  return jobs.filter(
    (job) =>
      (job.title && job.title.toLowerCase().includes(keywordLower)) ||
      (job.description && job.description.toLowerCase().includes(keywordLower))
  )
}

/**
 * Filter jobs by location
 */
export const filterJobsByLocation = (jobs, city, country) => {
  return jobs.filter((job) => {
    if (city && job.city && !job.city.toLowerCase().includes(city.toLowerCase())) {
      return false
    }
    if (country && job.country && !job.country.toLowerCase().includes(country.toLowerCase())) {
      return false
    }
    return true
  })
}

/**
 * Filter jobs by seniority level
 */
export const filterJobsBySeniority = (jobs, seniority) => {
  if (!seniority) return jobs

  const seniorityMap = {
    entry: ['entry', 'junior', 'graduate'],
    mid: ['mid', 'intermediate', 'experienced'],
    senior: ['senior', 'principal', 'lead'],
    lead: ['lead', 'manager', 'director'],
  }

  const levels = seniorityMap[seniority] || []

  return jobs.filter((job) => {
    if (!job.seniority) return true
    const level = job.seniority.toLowerCase()
    return levels.some((l) => level.includes(l))
  })
}

/**
 * Sort jobs by various criteria
 */
export const sortJobs = (jobs, sortBy = 'relevance', sortOrder = 'desc') => {
  const sorted = [...jobs]

  switch (sortBy) {
    case 'salary':
      sorted.sort((a, b) => (a.salary || 0) - (b.salary || 0))
      break
    case 'date':
      sorted.sort(
        (a, b) => new Date(b.posted_date) - new Date(a.posted_date)
      )
      break
    case 'title':
      sorted.sort((a, b) => (a.title || '').localeCompare(b.title || ''))
      break
    case 'relevance':
    default:
      // Keep original order or could implement relevance scoring
      break
  }

  if (sortOrder === 'asc') sorted.reverse()

  return sorted
}

/**
 * Apply all filters and sorting to jobs
 */
export const applyFilters = (jobs, filters) => {
  let filtered = jobs

  // Apply location filters
  if (filters.city || filters.country) {
    filtered = filterJobsByLocation(filtered, filters.city, filters.country)
  }

  // Apply salary filter
  if (filters.min_salary || filters.max_salary) {
    filtered = filterJobsBySalary(filtered, filters.min_salary, filters.max_salary)
  }

  // Apply keyword filter
  if (filters.keyword) {
    filtered = filterJobsByKeyword(filtered, filters.keyword)
  }

  // Apply seniority filter
  if (filters.seniority) {
    filtered = filterJobsBySeniority(filtered, filters.seniority)
  }

  // Apply skill filter
  if (filters.skills && filters.skills.length > 0) {
    filtered = filterJobsBySkills(
      filtered,
      filters.skills,
      filters.skill_match_type || 'any'
    )
  }

  // Apply sorting
  filtered = sortJobs(
    filtered,
    filters.sort_by || 'relevance',
    filters.sort_order || 'desc'
  )

  return filtered
}

/**
 * Calculate skill match score for a job
 */
export const calculateSkillMatchScore = (job, userSkills = []) => {
  if (!job.required_skills || userSkills.length === 0) {
    return 0
  }

  const userSkillsLower = userSkills.map((s) => s.toLowerCase())
  const matchedSkills = job.required_skills.filter((jobSkill) =>
    userSkillsLower.some((userSkill) => jobSkill.toLowerCase().includes(userSkill))
  )

  const matchPercentage = (matchedSkills.length / job.required_skills.length) * 100
  return Math.round(matchPercentage)
}

/**
 * Get job suggestions based on user profile
 */
export const suggestJobs = (jobs, userProfile) => {
  return jobs
    .map((job) => ({
      job,
      score: calculateRelevanceScore(job, userProfile),
    }))
    .filter(({ score }) => score > 0)
    .sort((a, b) => b.score - a.score)
    .map(({ job }) => job)
}

/**
 * Calculate relevance score for a job
 */
export const calculateRelevanceScore = (job, userProfile = {}) => {
  let score = 0

  // Skill match (0-40 points)
  const skillMatch = calculateSkillMatchScore(job, userProfile.skills)
  score += (skillMatch / 100) * 40

  // Salary match (0-30 points)
  if (userProfile.minSalary && userProfile.maxSalary && job.salary) {
    const midPoint = (userProfile.minSalary + userProfile.maxSalary) / 2
    const salaryDiff = Math.abs(job.salary - midPoint)
    const salaryRange = userProfile.maxSalary - userProfile.minSalary
    const salaryMatch = Math.max(0, 1 - salaryDiff / salaryRange)
    score += salaryMatch * 30
  }

  // Location match (0-20 points)
  if (userProfile.preferredLocation) {
    if (
      job.city?.toLowerCase().includes(userProfile.preferredLocation.toLowerCase()) ||
      job.country?.toLowerCase().includes(userProfile.preferredLocation.toLowerCase())
    ) {
      score += 20
    }
  }

  // Seniority match (0-10 points)
  if (userProfile.seniority && job.seniority) {
    if (job.seniority.toLowerCase().includes(userProfile.seniority.toLowerCase())) {
      score += 10
    }
  }

  return score
}

/**
 * Paginate items array
 */
export const paginate = (items, page = 1, limit = 20) => {
  const start = (page - 1) * limit
  const end = start + limit
  return {
    items: items.slice(start, end),
    total: items.length,
    pages: Math.ceil(items.length / limit),
    currentPage: page,
  }
}
