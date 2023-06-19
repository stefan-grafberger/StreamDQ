// Based on https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/util/typeutils/FieldAccessorFactory.java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.util.typeutils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType.InvalidFieldReferenceException
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import java.io.Serializable
import java.util.regex.Pattern

class NullCheckingFieldAccessorFactory : FieldAccessorFactory() {
    companion object {
        private val PATTERN_NESTED_FIELDS_WILDCARD = Pattern.compile("([\\p{L}\\p{Digit}_$]*)(\\.(.+))?|\\*|_")

        private fun decomposeFieldExpression(fieldExpression: String): FieldExpression {
            val matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression)
            return if (!matcher.matches()) {
                throw InvalidFieldReferenceException("Invalid field expression \"$fieldExpression\".")
            } else {
                var head = matcher.group(0)
                if (head != "*" && head != "_") {
                    head = matcher.group(1)
                    val tail = matcher.group(3)
                    FieldExpression(head, tail)
                } else {
                    throw InvalidFieldReferenceException("No wildcards are allowed here.")
                }
            }
        }

        private class FieldExpression(var head: String?, var tail: String?) : Serializable {
            companion object {
                private const val serialVersionUID = 1L
            }
        }

        private fun <T, F> getAccessor(
            typeInfo: TypeInformation<T>,
            pos: Int,
            config: ExecutionConfig?
        ): NullCheckingFieldAccessor<T, F> {
            val result = if (typeInfo !is BasicArrayTypeInfo<*, *> && typeInfo !is PrimitiveArrayTypeInfo<*>) {
                if (typeInfo is BasicTypeInfo<*>) {
                    if (pos != 0) {
                        throw InvalidFieldReferenceException(
                            "The " + Integer.valueOf(pos)
                                .toString() + ". field selected on a basic type (" + typeInfo.toString() +
                                "). A field expression on a basic type can only select the 0th field " +
                                "(which means selecting the entire basic type)."
                        )
                    } else {
                        NullCheckingSimpleFieldAccessor(typeInfo)
                    }
                } else if (typeInfo.isTupleType && (typeInfo as TupleTypeInfoBase<*>).isCaseClass) {
                    val tupleTypeInfo = typeInfo as TupleTypeInfoBase<*>
                    val fieldTypeInfo: TypeInformation<F?> = tupleTypeInfo.getTypeAt(pos)
                    NullCheckingRecursiveProductFieldAccessor(
                        pos,
                        typeInfo,
                        NullCheckingSimpleFieldAccessor(fieldTypeInfo),
                        config
                    )
                } else if (typeInfo.isTupleType) {
                    @Suppress("UNCHECKED_CAST")
                    NullCheckingSimpleTupleFieldAccessor(pos, typeInfo as TypeInformation<Tuple>)
                } else {
                    throw InvalidFieldReferenceException(
                        "Cannot reference field by position on " +
                            typeInfo.toString() + "Referencing a field by position is supported on tuples, " +
                            "case classes, and arrays. Additionally, you can select the 0th field of a " +
                            "primitive/basic type (e.g. int)."
                    )
                }
            } else {
                NullCheckingArrayFieldAccessor(pos, typeInfo)
            }
            @Suppress("UNCHECKED_CAST")
            return result as NullCheckingFieldAccessor<T, F>
        }

        fun <T, F> getAccessor(
            typeInfo: TypeInformation<T>,
            field: String,
            config: ExecutionConfig?
        ): NullCheckingFieldAccessor<T, F> {
            val result = if (typeInfo !is BasicArrayTypeInfo<*, *> && typeInfo !is PrimitiveArrayTypeInfo<*>) {
                if (typeInfo is BasicTypeInfo<*>) {
                    try {
                        val pos = if (field == "*") 0 else field.toInt()
                        this.getAccessor(typeInfo, pos, config)
                    } catch (var9: NumberFormatException) {
                        throw InvalidFieldReferenceException(
                            "You tried to select the field \"$field\" on a " +
                                "$typeInfo. A field expression on a basic type can only be \"*\" or \"0\" (both of " +
                                "which mean selecting the entire basic type)."
                        )
                    }
                } else {
                    var fieldPos: Int
                    if (typeInfo is PojoTypeInfo<*>) {
                        val decomposedFieldExpression = decomposeFieldExpression(field)
                        val pojoTypeInfo = typeInfo as PojoTypeInfo<*>
                        fieldPos = pojoTypeInfo.getFieldIndex(decomposedFieldExpression.head)
                        if (fieldPos == -1) {
                            throw InvalidFieldReferenceException(
                                "Unable to find field \"" +
                                    decomposedFieldExpression.head + "\" in type " + typeInfo + "."
                            )
                        } else {
                            val pojoField = pojoTypeInfo.getPojoFieldAt(fieldPos)
                            val fieldType = pojoTypeInfo.getTypeAt<Any>(fieldPos)
                            if (decomposedFieldExpression.tail == null) {
                                val innerAccessor = NullCheckingSimpleFieldAccessor(fieldType)
                                NullCheckingPojoFieldAccessor(pojoField.field, innerAccessor)
                            } else {
                                @Suppress("UNCHECKED_CAST")
                                val innerAccessor = getAccessor<T, F>(
                                    fieldType as TypeInformation<T>,
                                    decomposedFieldExpression.tail!!, config
                                )
                                NullCheckingPojoFieldAccessor(pojoField.field, innerAccessor)
                            }
                        }
                    } else {
                        val decomposedFieldExpression: FieldExpression
                        val innerAccessor: FieldAccessor<*, *>
                        if (typeInfo.isTupleType && (typeInfo as TupleTypeInfoBase<*>).isCaseClass) {
                            val tupleTypeInfo = typeInfo as TupleTypeInfoBase<*>
                            decomposedFieldExpression = decomposeFieldExpression(field)
                            fieldPos = tupleTypeInfo.getFieldIndex(decomposedFieldExpression.head)
                            when {
                                fieldPos < 0 -> {
                                    throw InvalidFieldReferenceException("Invalid field selected: $field")
                                }
                                decomposedFieldExpression.tail == null -> {
                                    NullCheckingSimpleProductFieldAccessor(fieldPos, typeInfo, config)
                                }
                                else -> {
                                    innerAccessor = this.getAccessor<Any, Any>(
                                        tupleTypeInfo.getTypeAt(fieldPos),
                                        decomposedFieldExpression.tail!!,
                                        config
                                    )
                                    NullCheckingRecursiveProductFieldAccessor(
                                        fieldPos,
                                        typeInfo,
                                        innerAccessor,
                                        config
                                    )
                                }
                            }
                        } else if (typeInfo.isTupleType && typeInfo is TupleTypeInfo<*>) {
                            val tupleTypeInfo = typeInfo as TupleTypeInfo<*>
                            decomposedFieldExpression = decomposeFieldExpression(field)
                            fieldPos = tupleTypeInfo.getFieldIndex(decomposedFieldExpression.head)
                            if (fieldPos == -1) {
                                fieldPos = try {
                                    decomposedFieldExpression.head!!.toInt()
                                } catch (var10: NumberFormatException) {
                                    throw InvalidFieldReferenceException(
                                        "Tried to select field \"" +
                                            decomposedFieldExpression.head + "\" on " + typeInfo.toString() +
                                            " . Only integer values are allowed here."
                                    )
                                }
                            }
                            if (decomposedFieldExpression.tail == null) {
                                NullCheckingSimpleTupleFieldAccessor(fieldPos, tupleTypeInfo)
                            } else {
                                innerAccessor = this.getAccessor<Any, Any>(
                                    tupleTypeInfo.getTypeAt(fieldPos),
                                    decomposedFieldExpression.tail!!,
                                    config
                                )
                                NullCheckingRecursiveTupleFieldAccessor(fieldPos, innerAccessor, tupleTypeInfo)
                            }
                        } else {
                            throw InvalidFieldReferenceException(
                                "Cannot reference field by field expression on " +
                                    typeInfo.toString() + "Field expressions are only supported on POJO types, " +
                                    "tuples, and case classes. (See the Flink documentation on what is considered " +
                                    "a POJO.)"
                            )
                        }
                    }
                }
            } else {
                try {
                    NullCheckingArrayFieldAccessor(field.toInt(), typeInfo)
                } catch (var11: NumberFormatException) {
                    throw InvalidFieldReferenceException(
                        "A field expression on an array must be an integer index " +
                            "(that might be given as a string)."
                    )
                }
            }
            @Suppress("UNCHECKED_CAST")
            return result as NullCheckingFieldAccessor<T, F>
        }
    }
}
