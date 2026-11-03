package com.example.orderdemo.api;

import com.example.orderdemo.infrastructure.trace.TraceContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/** 每个 HTTP 请求一个 traceId，事件信封会带上它。 */
@Component
public class TraceFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        TraceContext.set(UUID.randomUUID().toString().substring(0, 8));
        try {
            chain.doFilter(request, response);
        } finally {
            TraceContext.clear();
        }
    }
}
