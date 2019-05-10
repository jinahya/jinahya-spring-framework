package com.github.jinahya.springframework.web.bind;

import org.apache.commons.io.input.NullInputStream;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.Min;
import java.io.InputStream;

@RequestMapping(path = {"/bigBody"})
@RestController
public class BigBodyController {

    @GetMapping
    public ResponseEntity<InputStreamResource> read(
            @RequestParam(name = "size", defaultValue = "1048576") @Min(0) final long size) {
        final InputStream stream = new NullInputStream(size);
        final InputStreamResource resource = new InputStreamResource(stream);
        return ResponseEntity.ok(resource);
    }
}
