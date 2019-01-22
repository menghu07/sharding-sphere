package io.shardingsphere.core.parsing.parser.token;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Created by monis on 2018/11/30.
 * 标识表查询时按表顺序分页查询
 */
@Getter
@ToString
public final class NoOrderToken extends SQLToken {

    public NoOrderToken(final int beginPosition) {
        super(beginPosition);
    }
}
