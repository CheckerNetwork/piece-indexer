import { AssertionError } from 'node:assert'

export const assertResponseStatus = async (res, status) => {
  if (res.status !== status) {
    throw new AssertionError({
      actual: res.status,
      expected: status,
      message: `Unexpected status code ${res.status} (expected ${status}). Response body: ${await res.text() || '(empty)'}`
    })
  }
}
