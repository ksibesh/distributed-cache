package com.example.cache.controller;

import com.example.cache.controller.domain.GetResponse;
import com.example.cache.controller.domain.PutRequest;
import com.example.cache.controller.domain.PutResponse;
import com.example.cache.controller.domain.RemoveResponse;
import com.example.cache.core.IDistributedCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static com.example.cache.util.SystemUtil.DEFAULT_ERROR_CODE;

public class CacheControllerTest {
    private MockMvc mockMvc;
    private IDistributedCache<String, String> distributedCache;
    private ObjectMapper objectMapper;

    private final String CACHE_ENDPOINT = "/cache";
    private final String TEST_KEY = "testKey";
    private final String TEST_VALUE = "testValue";
    private final long TEST_TTL = 3600;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        objectMapper = new ObjectMapper();
        distributedCache = (IDistributedCache<String, String>) mock(IDistributedCache.class);

        CacheController cacheController = new CacheController(distributedCache);
        mockMvc = MockMvcBuilders.standaloneSetup(cacheController).build();
    }

    @Test
    public void testPutSuccess() throws Exception {
        PutRequest request = new PutRequest();
        request.setKey(TEST_KEY);
        request.setValue(TEST_VALUE);
        request.setTtlInSec(TEST_TTL);

        String responseContent = mockMvc.perform(put(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        verify(distributedCache, times(1)).put(TEST_KEY, TEST_VALUE, TEST_TTL);

        PutResponse response = objectMapper.readValue(responseContent, PutResponse.class);
        assertTrue(response.isPutStatus());
        assertNull(response.getErrorCode());
    }

    @Test
    public void testPutFailure() throws Exception {
        PutRequest request = new PutRequest();
        request.setKey(TEST_KEY);
        request.setValue(TEST_VALUE);
        request.setTtlInSec(TEST_TTL);

        String errorMessage = "Storage capacity exceeded.";
        doThrow(new RuntimeException(errorMessage)).when(distributedCache).put(TEST_KEY, TEST_VALUE, TEST_TTL);

        String responseContent = mockMvc.perform(put(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isInternalServerError())
                .andReturn().getResponse().getContentAsString();

        PutResponse response = objectMapper.readValue(responseContent, PutResponse.class);
        assertEquals(DEFAULT_ERROR_CODE, response.getErrorCode());
        assertEquals(errorMessage, response.getErrorMessage());
        assertFalse(response.isPutStatus());
    }

    @Test
    public void testGetSuccess() throws Exception {
        when(distributedCache.get(TEST_KEY)).thenReturn(TEST_VALUE);

        String responseContent = mockMvc.perform(get(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("key", TEST_KEY))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        verify(distributedCache, times(1)).get(TEST_KEY);
        GetResponse response = objectMapper.readValue(responseContent, GetResponse.class);
        assertNull(response.getErrorCode());
        assertEquals(TEST_VALUE, response.getValue());
    }

    @Test
    public void testGetSuccessWithCacheMiss() throws Exception {
        when(distributedCache.get(TEST_KEY)).thenReturn(null);

        String responseContent = mockMvc.perform(get(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("key", TEST_KEY))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        verify(distributedCache, times(1)).get(TEST_KEY);
        GetResponse response = objectMapper.readValue(responseContent, GetResponse.class);
        assertNull(response.getErrorCode());
        assertNull(response.getValue());
    }

    @Test
    public void testGetFailure() throws Exception {
        String errorMessage = "Internal server error while accessing cache.";
        doThrow(new RuntimeException(errorMessage)).when(distributedCache).get(TEST_KEY);

        String responseContent = mockMvc.perform(get(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("key", TEST_KEY))
                .andExpect(status().isInternalServerError())
                .andReturn().getResponse().getContentAsString();

        GetResponse response = objectMapper.readValue(responseContent, GetResponse.class);
        assertEquals(DEFAULT_ERROR_CODE, response.getErrorCode());
        assertEquals(errorMessage, response.getErrorMessage());
        assertNull(response.getValue());
    }

    @Test
    public void testRemoveSuccess() throws Exception {
        doNothing().when(distributedCache).delete(TEST_KEY);

        String responseContent = mockMvc.perform(delete(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("key", TEST_KEY))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        verify(distributedCache, times(1)).delete(TEST_KEY);
        RemoveResponse response = objectMapper.readValue(responseContent, RemoveResponse.class);
        assertTrue(response.isRemoveStatus());
        assertNull(response.getErrorCode());
    }

    @Test
    public void testRemoveFailure() throws Exception {
        String errorMessage = "Cache unavailable at moment.";
        doThrow(new RuntimeException(errorMessage)).when(distributedCache).delete(TEST_KEY);

        String responseContent = mockMvc.perform(delete(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("key", TEST_KEY))
                .andExpect(status().isInternalServerError())
                .andReturn().getResponse().getContentAsString();

        RemoveResponse response = objectMapper.readValue(responseContent, RemoveResponse.class);
        assertEquals(DEFAULT_ERROR_CODE, response.getErrorCode());
        assertEquals(errorMessage, response.getErrorMessage());
        assertFalse(response.isRemoveStatus());
    }
}
