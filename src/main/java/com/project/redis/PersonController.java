package com.project.redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
public class PersonController {
    @Autowired
    RedisTemplate<String, Person> redisTemplate;

    /**
     *   String ops
     **/
    @PostMapping("/string/person")
    public void savePerson(@RequestBody Person person) {
        String key = String.valueOf(person.getId());
        redisTemplate.opsForValue().set(key, person);
    }

    @GetMapping("/string/person")
    public Person getPerson(@RequestParam("id") int id) {
        return redisTemplate.opsForValue().get(id);
    }
}
