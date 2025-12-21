package com.example.cache.api;

import com.example.cache.api.advice.GlobalResponseAdvice;
import com.example.cache.api.domain.GetResponse;
import com.example.cache.api.domain.PutRequest;
import com.example.cache.api.domain.PutResponse;
import com.example.cache.api.domain.DeleteResponse;
import com.example.cache.core.IDistributedCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static com.example.cache.util.SystemUtil.DEFAULT_ERROR_CODE;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

public class CacheControllerTest {
    private MockMvc mockMvc;
    private IDistributedCache cacheCore;
    private ObjectMapper objectMapper;

    private final String CACHE_ENDPOINT = "/cache";
    private final String TEST_KEY = "testKey";
    private final String TEST_VALUE = "testValue";
    private final long TEST_TTL = 3600;

    @BeforeEach
    public void setup() {
        objectMapper = new ObjectMapper();
        cacheCore = mock(IDistributedCache.class);

        CacheController cacheController = new CacheController(cacheCore);
        mockMvc = MockMvcBuilders.standaloneSetup(cacheController)
                .setControllerAdvice(new GlobalResponseAdvice())
                .build();
    }

    @Test
    public void testPutSuccess() throws Exception {
        when(cacheCore.submitPut(TEST_KEY, TEST_VALUE, TEST_TTL)).thenReturn(CompletableFuture.completedFuture(null));

        PutRequest request = new PutRequest();
        request.setKey(TEST_KEY);
        request.setValue(TEST_VALUE);
        request.setTtlInSec(TEST_TTL);

        MvcResult mvcResult = mockMvc.perform(put(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(request().asyncStarted())
                .andReturn();

        String responseContent = mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        verify(cacheCore, times(1)).submitPut(TEST_KEY, TEST_VALUE, TEST_TTL);

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
        when(cacheCore.submitPut(TEST_KEY, TEST_VALUE, TEST_TTL))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException(errorMessage)));

        MvcResult mvcResult = mockMvc.perform(put(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(request().asyncStarted())
                .andReturn();

        String responseContent = mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isInternalServerError())
                .andReturn().getResponse().getContentAsString();

        PutResponse response = objectMapper.readValue(responseContent, PutResponse.class);
        assertEquals(DEFAULT_ERROR_CODE, response.getErrorCode());
        assertEquals(errorMessage, response.getErrorMessage());
        assertFalse(response.isPutStatus());
    }

    @Test
    public void testGetSuccess() throws Exception {
        when(cacheCore.submitGet(TEST_KEY)).thenReturn(CompletableFuture.completedFuture(TEST_VALUE));

        MvcResult mvcResult = mockMvc.perform(get(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("key", TEST_KEY))
                .andExpect(request().asyncStarted())
                .andReturn();
        String responseContent = mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        verify(cacheCore, times(1)).submitGet(TEST_KEY);
        GetResponse response = objectMapper.readValue(responseContent, GetResponse.class);
        assertNull(response.getErrorCode());
        assertEquals(TEST_VALUE, response.getValue());
    }

    @Test
    public void testGetSuccessWithCacheMiss() throws Exception {
        when(cacheCore.submitGet(TEST_KEY)).thenReturn(CompletableFuture.completedFuture(null));

        MvcResult mvcResult = mockMvc.perform(get(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("key", TEST_KEY))
                .andExpect(request().asyncStarted())
                .andReturn();
        String responseContent = mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        verify(cacheCore, times(1)).submitGet(TEST_KEY);
        GetResponse response = objectMapper.readValue(responseContent, GetResponse.class);
        assertNull(response.getErrorCode());
        assertNull(response.getValue());
    }

    @Test
    public void testGetFailure() throws Exception {
        String errorMessage = "Internal server error while accessing cache.";
        when(cacheCore.submitGet(TEST_KEY))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException(errorMessage)));

        MvcResult mvcResult = mockMvc.perform(get(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("key", TEST_KEY))
                .andExpect(request().asyncStarted())
                .andReturn();
        String responseContent = mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isInternalServerError())
                .andReturn().getResponse().getContentAsString();

        GetResponse response = objectMapper.readValue(responseContent, GetResponse.class);
        assertEquals(DEFAULT_ERROR_CODE, response.getErrorCode());
        assertEquals(errorMessage, response.getErrorMessage());
        assertNull(response.getValue());
    }

    @Test
    public void testRemoveSuccess() throws Exception {
        when(cacheCore.submitDelete(TEST_KEY)).thenReturn(CompletableFuture.completedFuture(null));

        MvcResult mvcResult = mockMvc.perform(delete(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("key", TEST_KEY))
                .andExpect(request().asyncStarted())
                .andReturn();
        String responseContent = mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        verify(cacheCore, times(1)).submitDelete(TEST_KEY);
        DeleteResponse response = objectMapper.readValue(responseContent, DeleteResponse.class);
        assertTrue(response.isRemoveStatus());
        assertNull(response.getErrorCode());
    }

    @Test
    public void testRemoveFailure() throws Exception {
        String errorMessage = "Cache unavailable at moment.";
        when(cacheCore.submitDelete(TEST_KEY))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException(errorMessage)));

        MvcResult mvcResult = mockMvc.perform(delete(CACHE_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("key", TEST_KEY))
                .andExpect(request().asyncStarted())
                .andReturn();
        String responseContent = mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isInternalServerError())
                .andReturn().getResponse().getContentAsString();

        DeleteResponse response = objectMapper.readValue(responseContent, DeleteResponse.class);
        assertEquals(DEFAULT_ERROR_CODE, response.getErrorCode());
        assertEquals(errorMessage, response.getErrorMessage());
        assertFalse(response.isRemoveStatus());
    }
}
