import { useFilterStore } from '@/stores/filterStore'
import { useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'

/**
 * Hook to sync filter store with URL search params
 * Bidirectional sync: URL params -> store and store -> URL params
 */
export const useUrlSync = () => {
  const [searchParams, setSearchParams] = useSearchParams()
  const store = useFilterStore()

  // Sync URL params to store on mount and when params change
  useEffect(() => {
    const paramsObj = Object.fromEntries(searchParams)

    // Parse array params (skills)
    if (paramsObj.skills && typeof paramsObj.skills === 'string') {
      paramsObj.skills = paramsObj.skills.split(',').filter(Boolean)
    }

    // Update store from URL
    store.setFromParams(paramsObj)
  }, [searchParams, store])

  // Sync store to URL when filters change
  useEffect(() => {
    const params = store.getFilterParams()
    const newParams = new URLSearchParams()

    Object.entries(params).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '' || (Array.isArray(value) && value.length === 0)) {
        return
      }

      if (Array.isArray(value)) {
        newParams.set(key, value.join(','))
      } else {
        newParams.set(key, value)
      }
    })

    // Only update if params actually changed
    const currentParams = Object.fromEntries(searchParams)
    const newParamsObj = Object.fromEntries(newParams)

    if (JSON.stringify(currentParams) !== JSON.stringify(newParamsObj)) {
      setSearchParams(newParams, { replace: false })
    }
  }, [store.getFilterParams()]) // This is intentionally loose to catch all changes

  return { searchParams, setSearchParams }
}

/**
 * Helper to get filter params from URL
 */
export const getFilterParamsFromUrl = (searchParams) => {
  const params = Object.fromEntries(searchParams)

  // Parse array params
  if (params.skills && typeof params.skills === 'string') {
    params.skills = params.skills.split(',').filter(Boolean)
  }

  // Parse number params
  if (params.page) params.page = parseInt(params.page, 10)
  if (params.limit) params.limit = parseInt(params.limit, 10)
  if (params.min_salary) params.min_salary = parseFloat(params.min_salary)
  if (params.max_salary) params.max_salary = parseFloat(params.max_salary)

  return params
}

/**
 * Helper to create URL with filter params
 */
export const createFilterUrl = (path, filters) => {
  const params = new URLSearchParams()

  Object.entries(filters).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') {
      return
    }

    if (Array.isArray(value)) {
      if (value.length > 0) {
        params.set(key, value.join(','))
      }
    } else {
      params.set(key, value)
    }
  })

  const queryString = params.toString()
  return queryString ? `${path}?${queryString}` : path
}
