import { create } from 'zustand'

/**
 * Global filter state using Zustand
 * Persists across page navigation with support for URL params
 */
export const useFilterStore = create((set, get) => ({
  // Location filters
  city: null,
  country: null,

  // Job filters
  seniority: null,
  category: null,
  keyword: '',

  // Skill filters
  skills: [],
  skillMatchType: 'any', // 'any' | 'all'

  // Salary filters
  minSalary: null,
  maxSalary: null,
  salaryCurrency: 'USD',

  // Pagination
  page: 1,
  limit: 20,

  // Sorting
  sortBy: 'relevance', // 'relevance', 'salary', 'date', 'title'
  sortOrder: 'desc', // 'asc' | 'desc'

  // Location Actions
  setCity: (city) => set({ city, page: 1 }),
  setCountry: (country) => set({ country, page: 1 }),

  // Job Actions
  setSeniority: (seniority) => set({ seniority, page: 1 }),
  setCategory: (category) => set({ category, page: 1 }),
  setKeyword: (keyword) => set({ keyword, page: 1 }),

  // Skill Actions
  setSkills: (skills) => set({ skills, page: 1 }),
  addSkill: (skill) => {
    const current = get().skills
    if (!current.includes(skill)) {
      set({ skills: [...current, skill], page: 1 })
    }
  },
  removeSkill: (skill) => {
    const current = get().skills
    set({ skills: current.filter((s) => s !== skill), page: 1 })
  },
  setSkillMatchType: (type) => set({ skillMatchType: type, page: 1 }),

  // Salary Actions
  setMinSalary: (minSalary) => set({ minSalary, page: 1 }),
  setMaxSalary: (maxSalary) => set({ maxSalary, page: 1 }),
  setSalaryCurrency: (currency) => set({ salaryCurrency: currency }),
  setSalaryRange: (min, max) => set({ minSalary: min, maxSalary: max, page: 1 }),

  // Pagination Actions
  setPage: (page) => set({ page }),
  setLimit: (limit) => set({ limit, page: 1 }),

  // Sorting Actions
  setSortBy: (sortBy) => set({ sortBy, page: 1 }),
  setSortOrder: (sortOrder) => set({ sortOrder }),
  setSort: (sortBy, sortOrder = 'desc') => set({ sortBy, sortOrder, page: 1 }),

  // Utility Actions
  reset: () =>
    set({
      city: null,
      country: null,
      seniority: null,
      category: null,
      keyword: '',
      skills: [],
      skillMatchType: 'any',
      minSalary: null,
      maxSalary: null,
      salaryCurrency: 'USD',
      page: 1,
      limit: 20,
      sortBy: 'relevance',
      sortOrder: 'desc',
    }),

  hasActiveFilters: () => {
    const state = get()
    return !!(
      state.city ||
      state.country ||
      state.seniority ||
      state.category ||
      state.keyword ||
      state.skills.length > 0 ||
      state.minSalary ||
      state.maxSalary
    )
  },

  // Get all filter params as object for API calls
  getFilterParams: () => {
    const state = get()
    return {
      city: state.city,
      country: state.country,
      seniority: state.seniority,
      category: state.category,
      keyword: state.keyword,
      skills: state.skills.length > 0 ? state.skills : undefined,
      skill_match_type: state.skills.length > 0 ? state.skillMatchType : undefined,
      min_salary: state.minSalary,
      max_salary: state.maxSalary,
      salary_currency: state.salaryCurrency,
      sort_by: state.sortBy,
      sort_order: state.sortOrder,
      page: state.page,
      limit: state.limit,
    }
  },

  // Set all filters from params object (useful for URL sync)
  setFromParams: (params) => {
    set({
      city: params.city || null,
      country: params.country || null,
      seniority: params.seniority || null,
      category: params.category || null,
      keyword: params.keyword || '',
      skills: Array.isArray(params.skills) ? params.skills : [],
      skillMatchType: params.skill_match_type || 'any',
      minSalary: params.min_salary || null,
      maxSalary: params.max_salary || null,
      salaryCurrency: params.salary_currency || 'USD',
      page: params.page ? parseInt(params.page, 10) : 1,
      limit: params.limit ? parseInt(params.limit, 10) : 20,
      sortBy: params.sort_by || 'relevance',
      sortOrder: params.sort_order || 'desc',
    })
  },
}))
